import io
import logging
from socket import gaierror
from urllib.parse import urljoin

from celery.states import SUCCESS, FAILURE, READY_STATES
from django.conf import settings
from django_celery_results.models import TaskResult
from django.core.files.base import ContentFile
from django.core.files.storage import storages
from django.db import transaction
from templated_email import send_templated_mail
from pyhmmer.easel import SequenceFile, MSAFile
from pyhmmer.plan7 import HMMFile

from hmmerapi.celery import app
from search.client import Client, HmmpgmdServerError
from result.models import Result, HitsIndex
from .models import HmmerJob, Database

logger = logging.getLogger(__name__)


@app.task(bind=True)
def run_search(self, job_id: str):
    logger.debug(f"Running job {job_id}")

    job = HmmerJob.objects.select_related("database").get(id=job_id)
    task_result = TaskResult.objects.get(task_id=self.request.id)
    job.task = task_result
    job.save(update_fields=["task"])

    if job.database.status == Database.StatusChoices.PAUSED:
        raise self.retry(
            exc=Exception(f"Searches for database '{job.database.id}' are paused"),
            max_retries=settings.HMMER.max_retries,
            countdown=settings.HMMER.retry_period_seconds,
        )

    try:
        db_config = settings.HMMER.databases[job.database.id]
    except KeyError:
        raise ValueError(f"Database {job.database.id} not found in settings")

    storage = storages["results"]

    path = storage.save(f"{job.id}/hits.bin", ContentFile(b""))

    try:
        with Client(address=db_config.host, port=db_config.port) as client:
            client.search(
                db_cmd=job.hmmpgmd_db,
                ranges=job.hmmpgmd_ranges,
                parameters=job.hmmpgmd_parameters,
                query=job.hmmpgmd_query,
                path=storage.path(path),
            )

        job.result_path = storage.path(path)
        job.save(update_fields=["result_path"])
        transaction.on_commit(lambda: job.post_process())

    except (HmmpgmdServerError, ConnectionError, gaierror) as e:
        logger.warning(e)
        storage.delete(f"{job.id}/hits.bin")

        raise self.retry(exc=e, max_retries=settings.HMMER.max_retries, countdown=settings.HMMER.retry_period_seconds)

    return storage.path(path)


@app.task(bind=True)
def schedule_next_iteration(self, job_id: str):
    job = HmmerJob.objects.select_related("database", "parent").get(id=job_id)

    logger.debug(f"Creating iteration {job.iteration + 1} from job {job_id}")

    if job.iteration == settings.HMMER.jackhmmer_max_iterations:
        logger.debug(
            f"Jackhmmer job {job_id} is iteration {job.iteration}/{settings.HMMER.jackhmmer_max_iterations}. Stopping."
        )

        return

    if job.iteration > 0:
        convergence_stats = job.convergence_stats

        logger.debug(convergence_stats)

        if convergence_stats["gained"] == 0 and convergence_stats["dropped"] == 0 and convergence_stats["lost"] == 0:
            logger.debug(f"Jackhmmer job {job_id} converged. Stopping.")

            return

    existing_job = job.get_first_child()

    if existing_job:
        existing_job.delete()

    next_job = job.clone()
    next_job.input_type = HmmerJob.InputChoices.UUID
    next_job.input = job_id

    next_job = job.add_child(instance=next_job)

    workflow = next_job.get_workflow()

    transaction.on_commit(lambda: workflow.delay())


@app.task(bind=True)
def schedule_batch_jobs(self, job_id: str):
    job = HmmerJob.objects.select_related("database", "parent").get(id=job_id)

    logger.debug(f"Creating child (batch) jobs from job {job_id}")

    if job.input_type == HmmerJob.InputChoices.MULTI_SEQUENCE:
        with SequenceFile(io.BytesIO(job.input.encode()), format="fasta") as fh:
            block = fh.read_block()

            for sequence in block:
                next_job = job.clone()

                next_job.input_type = HmmerJob.InputChoices.SEQUENCE
                next_job.input = f">{sequence.name.decode()} {sequence.description.decode()}\n{sequence.sequence}"

                next_job = job.add_child(instance=next_job)
                workflow = next_job.get_workflow(as_batch=True)
                transaction.on_commit(lambda: workflow.delay())

    elif job.input_type == HmmerJob.InputChoices.MULTI_HMM:
        with HMMFile(io.BytesIO(job.input.encode())) as fh:
            while (hmm := fh.read()) is not None:
                next_job = job.clone()

                next_job.input_type = HmmerJob.InputChoices.HMM

                with io.BytesIO() as hmm_fh:
                    hmm.write(hmm_fh, binary=False)
                    bytes = hmm_fh.getvalue()
                    next_job.input = bytes.decode()

                next_job = job.add_child(instance=next_job)
                workflow = next_job.get_workflow(as_batch=True)
                transaction.on_commit(lambda: workflow.delay())

    elif job.input_type == HmmerJob.InputChoices.MULTI_MSA:
        with MSAFile(io.BytesIO(job.input.encode())) as fh:
            while (msa := fh.read()) is not None:
                next_job = job.clone()

                next_job.input_type = HmmerJob.InputChoices.MSA

                with io.BytesIO() as msa_fh:
                    msa.write(msa_fh, format=fh.format)
                    bytes = msa_fh.getvalue()
                    next_job.input = bytes.decode()

                next_job = job.add_child(instance=next_job)
                workflow = next_job.get_workflow(as_batch=True)
                transaction.on_commit(lambda: workflow.delay())

    else:
        raise Exception(f"Cannot schedule batch job with input type '{job.input_type}'")


@app.task(bind=True)
def notify_on_job_completion(self, job_id: str):
    job = HmmerJob.objects.get(id=job_id)

    if job.email_address is None:
        # Check if root job has email address
        root_job = job.get_root()

        if root_job.email_address is None:
            return

        if not root_job.is_batch_mode:
            return

        should_send_email = False
        email_address = root_job.email_address

        if job.algo == HmmerJob.AlgoChoices.JACKHMMER:
            convergence_stats = job.convergence_stats
            current_iteration = job.iteration
            iterations = root_job.iterations

            reached_requested_iterations = current_iteration == iterations
            reached_convergence = (
                convergence_stats["gained"] == 0
                and convergence_stats["dropped"] == 0
                and convergence_stats["lost"] == 0
            )

            if reached_requested_iterations or reached_convergence:
                base = urljoin(settings.DJANGO.host_url, settings.DJANGO.base_url)
                result_url = urljoin(base, f"results/{root_job.id}")
                template_name = "jackhmmer"
                should_send_email = True

                context = {
                    "job": root_job,
                    "result_url": result_url,
                    "reached_convergence": reached_convergence,
                }

                root_job.email_address = None
                root_job.save(update_fields=["email_address"])
        else:
            all_ready = True

            for sub_job in root_job.get_children():
                if sub_job.task is None or sub_job.task.status not in READY_STATES:
                    all_ready = False
                    break

            if all_ready:
                base = urljoin(settings.DJANGO.host_url, settings.DJANGO.base_url)
                result_url = urljoin(base, f"results/{root_job.id}")
                template_name = "batch"
                should_send_email = True

                context = {
                    "job": root_job,
                    "result_url": result_url,
                }

                root_job.email_address = None
                root_job.save(update_fields=["email_address"])

        try:
            if should_send_email:
                send_templated_mail(
                    template_name=template_name,
                    from_email="noreply@ebi.ac.uk",
                    recipient_list=[email_address],
                    context=context,
                )
        except Exception:
            pass
        finally:
            return

    if len(job.email_address) == 0:
        return

    try:
        if job.task is None:
            return

        if job.task.status not in {SUCCESS, FAILURE}:
            return

        template_name = f"job/{job.task.status.lower()}"
        base = urljoin(settings.DJANGO.host_url, settings.DJANGO.base_url)
        result_url = urljoin(base, f"results/{job.id}")

        send_templated_mail(
            template_name=template_name,
            from_email="noreply@ebi.ac.uk",
            recipient_list=[job.email_address],
            context={
                "job": job,
                "result_url": result_url,
            },
        )
    except Exception:
        pass
    finally:
        job.email_address = None
        job.save(update_fields=["email_address"])


@app.task(bind=True)
def index_hits(self, job_id: str):
    logger.debug(f"Running indexing job {job_id}")

    job = HmmerJob.objects.select_related("database").get(id=job_id)

    try:
        db_config = settings.HMMER.databases[job.database.id]
    except KeyError:
        raise ValueError(f"Database {job.database.id} not found in settings")

    result, _ = Result.from_file(job.result_path, db_conf=db_config)
    index = HitsIndex(result)

    storage = storages["results"]
    path = storage.save(f"{job.id}/index.pkl", ContentFile(b""))

    index.to_file(storage.path(path))
    job.hits_index_path = storage.path(path)

    job.save(update_fields=["hits_index_path"])
