"""
app/routes/documents_router.py
"""

import os
import tempfile
from datetime import datetime, timezone

from fastapi import APIRouter, File, Form, HTTPException, UploadFile

from app.models.documents import DocType, FinancialDocument, ParseStatus

router = APIRouter(prefix="/documents", tags=["documents"])


@router.post("/upload/cas")
async def upload_cas(
    file: UploadFile = File(...),
    password: str = Form(...),
):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Only PDF files are accepted")

    contents = await file.read()

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp:
        tmp.write(contents)
        tmp_path = tmp.name

    doc = FinancialDocument(
        original_filename=file.filename,
        storage_path=tmp_path,
        file_size_bytes=len(contents),
        doc_type=DocType.CAS,
        parse_status=ParseStatus.QUEUED,
        upload_source="web",
        uploaded_at=datetime.now(timezone.utc),
    )
    await doc.insert()

    try:
        import json as _json
        import casparser as _casparser

        # Parse once — pass the result into the ingestion function
        _raw = _casparser.read_cas_pdf(tmp_path, password, output="json")
        _parsed = _json.loads(_raw)
        _file_type = _parsed.get("file_type", "")

        import logging
        logging.getLogger(__name__).info(
            f"CAS detected: file_type={_file_type}, "
            f"folios={len(_parsed.get('folios', []))}, "
            f"accounts={len(_parsed.get('accounts', []))}"
        )

        from app.integrations.cas_parser import (
            ingest_cams_kfintech_from_parsed,
            ingest_nsdl_cdsl_from_parsed,
        )

        if _file_type in ("CAMS", "KARVY", "KFINTECH"):
            result = await ingest_cams_kfintech_from_parsed(
                parsed=_parsed,
                document_id=str(doc.id),
            )
        else:
            result = await ingest_nsdl_cdsl_from_parsed(
                parsed=_parsed,
                document_id=str(doc.id),
            )

        return {
            "status": "success",
            "document_id": str(doc.id),
            "file_type": _file_type,
            "result": result,
        }
    except Exception as e:
        import traceback
        raise HTTPException(status_code=422, detail=f"{str(e)}\n{traceback.format_exc()}")
    finally:
        if os.path.exists(tmp_path):
            os.unlink(tmp_path)


@router.get("/")
async def list_documents():
    docs = await FinancialDocument.find_all().sort([("uploaded_at", -1)]).to_list()
    return [
        {
            "id": str(d.id),
            "filename": d.original_filename,
            "doc_type": d.doc_type,
            "parse_status": d.parse_status,
            "holdings_updated": d.holdings_updated,
            "uploaded_at": d.uploaded_at.isoformat() if d.uploaded_at else None,
        }
        for d in docs
    ]


@router.get("/{document_id}/status")
async def document_status(document_id: str):
    doc = await FinancialDocument.get(document_id)
    if not doc:
        raise HTTPException(status_code=404, detail="Document not found")
    return {
        "id": str(doc.id),
        "filename": doc.original_filename,
        "parse_status": doc.parse_status,
        "parse_error": doc.parse_error,
        "holdings_updated": doc.holdings_updated,
        "transactions_created": doc.transactions_created,
        "parsed_at": doc.parsed_at.isoformat() if doc.parsed_at else None,
    }