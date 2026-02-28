"""
Google Cloud Storage integration for the Royalty Consolidator.
Handles upload/download/delete of statements, exports, and contracts.
Graceful degradation: all public functions return sensible defaults when GCS is unavailable.
"""

import logging
import os
import tempfile
from typing import Optional, Union

log = logging.getLogger('royalty')

_client = None
_bucket = None
_bucket_name = ''


# ---------------------------------------------------------------------------
# Initialisation
# ---------------------------------------------------------------------------

def init_gcs() -> bool:
    """Initialise GCS client and bucket. Returns True on success."""
    global _client, _bucket, _bucket_name

    bucket_name = os.getenv('GCS_BUCKET', '')
    if not bucket_name:
        log.info("GCS_BUCKET not set — GCS disabled")
        return False

    try:
        from google.cloud import storage as gcs_storage

        creds_path = os.getenv('GOOGLE_APPLICATION_CREDENTIALS', '')
        if creds_path and os.path.isfile(creds_path):
            _client = gcs_storage.Client.from_service_account_json(creds_path)
        else:
            # Try default credentials (e.g. GCE metadata, workload identity)
            _client = gcs_storage.Client()

        _bucket = _client.bucket(bucket_name)
        # Quick connectivity check
        _bucket.reload()
        _bucket_name = bucket_name
        log.info("GCS initialised: bucket=%s", bucket_name)
        return True
    except Exception as e:
        log.warning("GCS unavailable: %s", e)
        _client = None
        _bucket = None
        return False


def is_available() -> bool:
    """Check if GCS is ready."""
    return _bucket is not None


# ---------------------------------------------------------------------------
# Upload functions (return GCS path string)
# ---------------------------------------------------------------------------

def _upload(gcs_path: str, source: Union[str, bytes, object],
            content_type: str = 'application/octet-stream') -> str:
    """Upload a file to GCS. source can be a local file path, bytes, or file-like object."""
    if _bucket is None:
        raise RuntimeError("GCS not initialised")

    blob = _bucket.blob(gcs_path)

    if isinstance(source, str) and os.path.isfile(source):
        blob.upload_from_filename(source, content_type=content_type)
    elif isinstance(source, bytes):
        blob.upload_from_string(source, content_type=content_type)
    elif hasattr(source, 'read'):
        blob.upload_from_file(source, content_type=content_type)
    else:
        raise ValueError(f"Unsupported source type: {type(source)}")

    log.info("GCS upload: %s (%d bytes)", gcs_path, blob.size or 0)
    return gcs_path


def upload_statement(deal_slug: str, payor_code: str, filename: str,
                     source: Union[str, bytes, object]) -> str:
    """Upload a statement file. Returns GCS path."""
    gcs_path = f"statements/{deal_slug}/{payor_code}/{filename}"
    ct = _guess_content_type(filename)
    return _upload(gcs_path, source, content_type=ct)


def upload_export(deal_slug: str, filename: str,
                  source: Union[str, bytes, object]) -> str:
    """Upload an export file. Returns GCS path."""
    gcs_path = f"exports/{deal_slug}/{filename}"
    ct = _guess_content_type(filename)
    return _upload(gcs_path, source, content_type=ct)


def upload_per_payor_export(deal_slug: str, filename: str,
                            source: Union[str, bytes, object]) -> str:
    """Upload a per-payor export file. Returns GCS path."""
    gcs_path = f"exports/{deal_slug}/per_payor/{filename}"
    ct = _guess_content_type(filename)
    return _upload(gcs_path, source, content_type=ct)


def upload_contract(deal_slug: str, payor_code: str, filename: str,
                    source: Union[str, bytes, object]) -> str:
    """Upload a contract PDF. Returns GCS path."""
    gcs_path = f"contracts/{deal_slug}/{payor_code}/{filename}"
    return _upload(gcs_path, source, content_type='application/pdf')


# ---------------------------------------------------------------------------
# Download functions
# ---------------------------------------------------------------------------

def download_to_tempfile(gcs_path: str) -> str:
    """Download a GCS object to a local temp file. Returns local path."""
    if _bucket is None:
        raise RuntimeError("GCS not initialised")

    blob = _bucket.blob(gcs_path)
    _, ext = os.path.splitext(gcs_path)
    fd, local_path = tempfile.mkstemp(suffix=ext)
    os.close(fd)

    blob.download_to_filename(local_path)
    log.info("GCS download: %s -> %s", gcs_path, local_path)
    return local_path


def download_to_bytes(gcs_path: str) -> bytes:
    """Download a GCS object as bytes."""
    if _bucket is None:
        raise RuntimeError("GCS not initialised")

    blob = _bucket.blob(gcs_path)
    return blob.download_as_bytes()


# ---------------------------------------------------------------------------
# Cleanup
# ---------------------------------------------------------------------------

def delete_deal_files(deal_slug: str) -> int:
    """Delete all GCS objects for a deal (statements + exports + contracts). Returns count deleted."""
    if _bucket is None:
        return 0

    count = 0
    prefixes = [
        f"statements/{deal_slug}/",
        f"exports/{deal_slug}/",
        f"contracts/{deal_slug}/",
    ]
    for prefix in prefixes:
        blobs = list(_bucket.list_blobs(prefix=prefix))
        for blob in blobs:
            blob.delete()
            count += 1

    if count:
        log.info("GCS cleanup: deleted %d objects for deal %s", count, deal_slug)
    return count


def delete_blob(gcs_path: str) -> bool:
    """Delete a single GCS object. Returns True if deleted."""
    if _bucket is None:
        return False
    try:
        blob = _bucket.blob(gcs_path)
        blob.delete()
        return True
    except Exception as e:
        log.warning("GCS delete failed for %s: %s", gcs_path, e)
        return False


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def create_upload_session(gcs_path: str, content_type: str = 'application/octet-stream',
                          origin: str = '') -> dict:
    """Create a resumable upload session URL for direct browser-to-GCS upload.
    Returns {upload_url, gcs_path}."""
    if _bucket is None:
        raise RuntimeError("GCS not initialised")
    blob = _bucket.blob(gcs_path)
    url = blob.create_resumable_upload_session(content_type=content_type, origin=origin)
    return {'upload_url': url, 'gcs_path': gcs_path}


def blob_exists(gcs_path: str) -> bool:
    """Quick existence check for a GCS blob."""
    if _bucket is None:
        return False
    return _bucket.blob(gcs_path).exists()


def download_to_file(gcs_path: str, local_path: str) -> str:
    """Download a GCS object to a specific local file path. Returns local path."""
    if _bucket is None:
        raise RuntimeError("GCS not initialised")
    blob = _bucket.blob(gcs_path)
    blob.download_to_filename(local_path)
    log.info("GCS download: %s -> %s", gcs_path, local_path)
    return local_path


def list_deal_exports(deal_slug: str) -> list:
    """List all export GCS paths for a deal. Returns list of gcs_path strings."""
    if _bucket is None:
        return []
    prefix = f"exports/{deal_slug}/"
    return [blob.name for blob in _bucket.list_blobs(prefix=prefix)]


def download_exports_to_dir(deal_slug: str, local_dir: str) -> dict:
    """Download all deal exports from GCS to a local directory.
    Returns {filename: local_path} for consolidated files found.
    """
    result = {'xlsx': None, 'csv': None, 'per_payor': {}}
    paths = list_deal_exports(deal_slug)
    if not paths:
        return result

    os.makedirs(local_dir, exist_ok=True)
    pp_dir = os.path.join(local_dir, 'per_payor')

    for gcs_path in paths:
        filename = os.path.basename(gcs_path)
        if not filename:
            continue
        is_per_payor = '/per_payor/' in gcs_path
        if is_per_payor:
            os.makedirs(pp_dir, exist_ok=True)
            local_path = os.path.join(pp_dir, filename)
        else:
            local_path = os.path.join(local_dir, filename)

        try:
            download_to_file(gcs_path, local_path)
        except Exception as e:
            log.warning("Failed to download %s: %s", gcs_path, e)
            continue

        fl = filename.lower()
        if is_per_payor:
            code = filename.split('_')[0] if '_' in filename else filename.rsplit('.', 1)[0]
            result['per_payor'][code] = local_path
        elif fl.endswith('.xlsx') and ('consolidated' in fl or fl.startswith('consolidated')):
            result['xlsx'] = local_path
        elif fl.endswith('.csv') and ('consolidated' in fl or fl.startswith('consolidated')):
            result['csv'] = local_path

    return result


def _guess_content_type(filename: str) -> str:
    """Guess content type from file extension."""
    ext = os.path.splitext(filename)[1].lower()
    return {
        '.xlsx': 'application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
        '.xls': 'application/vnd.ms-excel',
        '.csv': 'text/csv',
        '.pdf': 'application/pdf',
        '.zip': 'application/zip',
    }.get(ext, 'application/octet-stream')
