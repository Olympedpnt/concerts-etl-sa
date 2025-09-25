# concerts_etl/core/gsheet.py
import os
import asyncio
from datetime import datetime
from typing import List, Dict, Any

import gspread
from google.oauth2.service_account import Credentials

from concerts_etl.core.config import settings

_SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive.file",
    "https://www.googleapis.com/auth/drive",
]

# Ordre de colonnes souhaité pour le consolidé
_BASE_HEADERS = [
    "event_name",
    "event_datetime_local",
    "shotgun_tickets_sold",
    "dice_tickets_sold",
]

_client = None


def _datetime_to_str(v: Any) -> Any:
    if isinstance(v, datetime):
        # On garde ISO sans timezone si naive, sinon isoformat complet
        try:
            return v.isoformat()
        except Exception:
            return str(v)
    return v


def _ensure_client():
    global _client
    if _client is not None:
        return _client

    sa_path = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS", "")
    if not sa_path or not os.path.exists(sa_path):
        raise RuntimeError(
            "GOOGLE_APPLICATION_CREDENTIALS introuvable. "
            "Assure-toi que le secret GCP_SA_JSON est bien injecté et que l’étape Configure env l’écrit dans ce chemin."
        )

    creds = Credentials.from_service_account_file(sa_path, scopes=_SCOPES)
    _client = gspread.authorize(creds)
    return _client


def _open_spreadsheet():
    client = _ensure_client()
    if settings.gsheet_id:
        try:
            return client.open_by_key(settings.gsheet_id)
        except Exception:
            pass
    # fallback par titre (doit être partagé avec le compte de service)
    return client.open(settings.gsheet_doc_title or "Concerts Pointages")


def _get_or_create_worksheet(sh, title: str):
    try:
        return sh.worksheet(title)
    except gspread.WorksheetNotFound:
        return sh.add_worksheet(title=title, rows=1000, cols=26)


def _build_headers(rows: List[Dict[str, Any]]) -> List[str]:
    # Colonnes fixes d’abord, puis toute autre clé rencontrée (stable et déterministe)
    extra_keys = []
    seen = set(_BASE_HEADERS)
    for r in rows:
        for k in r.keys():
            if k not in seen:
                extra_keys.append(k)
                seen.add(k)
    # On trie les extras pour stabilité
    extra_keys_sorted = sorted(extra_keys)
    return _BASE_HEADERS + extra_keys_sorted


def _rows_to_matrix(rows: List[Dict[str, Any]], headers: List[str]) -> List[List[Any]]:
    out = []
    for r in rows:
        out.append([_datetime_to_str(r.get(h, "")) for h in headers])
    return out


async def export_to_gsheet(consolidated_rows: List[Dict[str, Any]]):
    """
    Écrit la table consolidée dans le Google Sheet.
    - Onglet = settings.gsheet_worksheet (ex: "shotgun_events" ou "consolidated")
    - Colonnes = event_name, event_datetime_local, shotgun_tickets_sold, dice_tickets_sold, + extras
    """
    if not consolidated_rows:
        # Rien à écrire → on vide quand même la feuille pour clarté ?
        # Ici on sort sans rien faire.
        return

    def _blocking():
        sh = _open_spreadsheet()
        ws = _get_or_create_worksheet(sh, settings.gsheet_worksheet or "consolidated")

        headers = _build_headers(consolidated_rows)
        matrix = _rows_to_matrix(consolidated_rows, headers)

        # Clear + rewrite
        ws.clear()
        ws.update("A1", [headers])           # en-têtes
        if matrix:
            ws.update(f"A2", matrix)         # données

        # Redimensionnement sommaire
        try:
            ws.resize(rows=max(1000, len(matrix) + 10), cols=max(26, len(headers)))
        except Exception:
            pass

    await asyncio.to_thread(_blocking)
