import mimetypes
import posixpath
from pathlib import Path

from django.conf import settings
from django.utils._os import safe_join
from django.utils.translation import gettext as _
from django.http import FileResponse, Http404, HttpResponseNotModified
from django.utils.http import http_date
from django.views.static import was_modified_since


def serve(request, path):
    path = posixpath.normpath(path).lstrip("/")
    document_root = settings.HMMER.downloads_storage_location
    fullpath = Path(safe_join(document_root, path))

    if not fullpath.exists():
        raise Http404(_("“%(path)s” does not exist") % {"path": fullpath})

    statobj = fullpath.stat()

    if not was_modified_since(request.META.get("HTTP_IF_MODIFIED_SINCE"), statobj.st_mtime):
        return HttpResponseNotModified()

    content_type, encoding = mimetypes.guess_type(str(fullpath))

    content_type = content_type or "application/octet-stream"

    if encoding == "gzip":
        content_type = "application/gzip"

    response = FileResponse(fullpath.open("rb"), as_attachment=True, content_type=content_type)
    response.headers["Last-Modified"] = http_date(statobj.st_mtime)

    return response
