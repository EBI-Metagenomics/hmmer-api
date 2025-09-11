FROM ghcr.io/astral-sh/uv:python3.13-bookworm AS builder

ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy

WORKDIR /app

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --frozen --no-install-project --no-dev

ADD . /app

RUN --mount=type=cache,target=/root/.cache/uv \
    uv sync --frozen --no-dev


FROM python:3.13-bookworm

# This is to enable NFS share access
ENV USER=docker
ENV UID=4050
ENV GID=1223
RUN addgroup --gid "$GID" "$USER" \
  && adduser \
  --disabled-password \
  --gecos "" \
  --home "$(pwd)" \
  --ingroup "$USER" \
  --no-create-home \
  --uid "$UID" \
  "$USER"

COPY --from=builder --chown=app:app /app /app

ENV LD_LIBRARY_PATH=/usr/local/lib
ENV CFLAGS="-fPIC"

# setup HMMER library
ARG HMMER_REPO=https://github.com/EBI-Metagenomics/hmmer
ARG HMMER_BRANCH=hmmpgmd2msa-fix
ARG EASEL_REPO=https://github.com/EddyRivasLab/easel
ARG EASEL_BRANCH=master

WORKDIR /opt
RUN git clone -b ${HMMER_BRANCH} ${HMMER_REPO}
WORKDIR /opt/hmmer
RUN git clone -b ${EASEL_BRANCH} ${EASEL_REPO}

RUN autoconf && ./configure
RUN make -j8
RUN gcc -g -fPIC -shared \
  -o /usr/local/lib/libhmmer.so \
  $(find src -maxdepth 1 -name '*.o' \
    ! -name 'alimask.o' \
    ! -name 'hmmconvert.o' \
    ! -name 'hmmalign.o' \
    ! -name 'hmmc2.o' \
    ! -name 'hmmpgmd.o' \
    ! -name 'hmmemit.o' \
    ! -name 'hmmfetch.o' \
    ! -name 'hmmlogo.o' \
    ! -name 'hmmerfm-exactmatch.o' \
    ! -name 'hmmpgmd_shard.o' \
    ! -name 'hmmpress.o' \
    ! -name 'hmmbuild.o' \
    ! -name 'hmmscan.o' \
    ! -name 'hmmsim.o' \
    ! -name 'hmmsearch.o' \
    ! -name 'hmmstat.o' \
    ! -name 'makehmmerdb.o' \
    ! -name 'nhmmscan.o' \
    ! -name 'jackhmmer.o' \
    ! -name 'nhmmer.o' \
    ! -name 'phmmer.o') \
  src/impl/*.o \
  easel/*.o \
  libdivsufsort/*.o \
  -lpthread -lm

RUN ldconfig

WORKDIR /app

ENV PATH="/app/.venv/bin:$PATH"

EXPOSE 8000
RUN ./manage.py collectstatic --noinput
CMD ["gunicorn", "--bind", ":8000", "--workers", "3", "--access-logfile", "-", "--log-file", "-", "hmmerapi.wsgi"]