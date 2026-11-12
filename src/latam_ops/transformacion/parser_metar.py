"""Parser de METAR crudo (texto no estructurado) a campos estructurados.

Se aplica solo al cuerpo principal del reporte (antes de RMK y de los grupos de tendencia
NOSIG/TEMPO/BECMG). La presencia de TEMPO/BECMG se conserva como señal de inestabilidad pronosticada.
Referencia de formato: OMM Manual de Claves 306, FM 15 METAR.
"""

from __future__ import annotations

import re
from typing import Any

_RE_VIENTO = re.compile(r"^(\d{3}|VRB)(\d{2,3})(?:G(\d{2,3}))?(KT|MPS)$")
_RE_VAR = re.compile(r"^\d{3}V\d{3}$")
_RE_VIS = re.compile(r"^(\d{4})(NDV)?$")
_RE_VIS_SM = re.compile(r"^(?:(\d{1,2}) )?(?:(\d{1,2})/(\d{1,2})|(\d{1,2}))SM$")
_RE_NUBE = re.compile(r"^(FEW|SCT|BKN|OVC)(\d{3}|///)(CB|TCU|///)?$")
_RE_VV = re.compile(r"^VV(\d{3}|///)$")
_RE_TEMP = re.compile(r"^(M?\d{2})/(M?\d{2})?$")
_RE_QNH = re.compile(r"^Q(\d{4})$")
_RE_ALT = re.compile(r"^A(\d{4})$")
_RE_FEN = re.compile(
    r"^(\+|-|VC)?(MI|PR|BC|DR|BL|SH|TS|FZ)?((?:DZ|RA|SN|SG|IC|PL|GR|GS|UP|BR|FG|FU|VA|DU|SA|HZ|PY|PO|SQ|FC|SS|DS)*)$"
)
_TENDENCIAS = {"NOSIG", "TEMPO", "BECMG"}
_NUBES_SIN_TECHO = {"NSC", "NCD", "SKC", "CLR"}
_M_POR_MILLA = 1609.344

CAMPOS: list[tuple[str, str]] = [
    ("tipo_reporte", "string"),
    ("viento_dir", "integer"),
    ("viento_variable", "boolean"),
    ("viento_kt", "integer"),
    ("rafaga_kt", "integer"),
    ("vis_m", "integer"),
    ("cavok", "boolean"),
    ("techo_ft", "integer"),
    ("nube_min_ft", "integer"),
    ("temp_c", "integer"),
    ("rocio_c", "integer"),
    ("qnh_hpa", "double"),
    ("fen_niebla", "integer"),
    ("fen_neblina", "integer"),
    ("fen_bruma", "integer"),
    ("fen_lluvia", "integer"),
    ("fen_llovizna", "integer"),
    ("fen_chubasco", "integer"),
    ("fen_tormenta", "integer"),
    ("fen_nieve", "integer"),
    ("fenomenos", "string"),
    ("tendencia", "string"),
    ("tiene_tempo", "integer"),
    ("categoria_vuelo", "string"),
    ("parseado_ok", "boolean"),
]


def esquema_spark():
    """StructType de salida del parser (import perezoso de pyspark)."""
    from pyspark.sql import types as T

    tipos = {"string": T.StringType(), "integer": T.IntegerType(), "boolean": T.BooleanType(), "double": T.DoubleType()}
    return T.StructType([T.StructField(n, tipos[t], True) for n, t in CAMPOS])


def _temp(valor: str | None) -> int | None:
    if not valor:
        return None
    return -int(valor[1:]) if valor.startswith("M") else int(valor)


def categoria_vuelo(techo_ft: int | None, vis_m: int | None) -> str | None:
    """Categoría FAA por la peor condición entre techo (ft) y visibilidad (millas)."""
    if techo_ft is None and vis_m is None:
        return None
    millas = vis_m / _M_POR_MILLA if vis_m is not None else None

    def peor(umbral_techo: float, umbral_vis: float, inclusivo: bool = False) -> bool:
        t = techo_ft is not None and (techo_ft <= umbral_techo if inclusivo else techo_ft < umbral_techo)
        v = millas is not None and (millas <= umbral_vis if inclusivo else millas < umbral_vis)
        return t or v

    if peor(500, 1):
        return "LIFR"
    if peor(1000, 3):
        return "IFR"
    if peor(3000, 5, inclusivo=True):
        return "MVFR"
    return "VFR"


def parsear_metar(texto: str | None) -> dict[str, Any]:
    """Convierte un METAR en un diccionario con los campos de CAMPOS (los no encontrados quedan en None)."""
    r: dict[str, Any] = {n: None for n, _ in CAMPOS}
    r.update({"viento_variable": False, "cavok": False, "tiene_tempo": 0, "parseado_ok": False})
    for f in ("fen_niebla", "fen_neblina", "fen_bruma", "fen_lluvia", "fen_llovizna", "fen_chubasco", "fen_tormenta", "fen_nieve"):
        r[f] = 0
    if not texto:
        return r
    cuerpo = texto.strip().rstrip("=").split(" RMK")[0]
    tokens = cuerpo.split()
    r["tiene_tempo"] = int(any(t in ("TEMPO", "BECMG") for t in tokens))
    idx_trend = next((i for i, t in enumerate(tokens) if t in _TENDENCIAS), len(tokens))
    tendencia = tokens[idx_trend] if idx_trend < len(tokens) else None
    r["tendencia"] = tendencia
    principal = tokens[:idx_trend]

    i = 0
    if principal and principal[0] in ("METAR", "SPECI"):
        r["tipo_reporte"] = principal[0]
        i = 1
    else:
        r["tipo_reporte"] = "METAR"
    estacion_ok = i < len(principal) and re.fullmatch(r"[A-Z0-9]{4}", principal[i] or "") is not None
    i += 1  # estación
    if i < len(principal) and re.fullmatch(r"\d{6}Z", principal[i]):
        i += 1
    techos: list[int] = []
    nubes: list[int] = []
    fenomenos: list[str] = []
    encontro_algo = False
    resto = principal[i:]
    j = 0
    while j < len(resto):
        t = resto[j]
        if t in ("AUTO", "COR", "NIL"):
            j += 1
            continue
        m = _RE_VIENTO.match(t)
        if m:
            factor = 1.94384 if m.group(4) == "MPS" else 1.0
            r["viento_dir"] = None if m.group(1) == "VRB" else int(m.group(1))
            r["viento_variable"] = m.group(1) == "VRB"
            r["viento_kt"] = round(int(m.group(2)) * factor)
            r["rafaga_kt"] = round(int(m.group(3)) * factor) if m.group(3) else None
            encontro_algo = True
        elif _RE_VAR.match(t):
            r["viento_variable"] = True
        elif t == "CAVOK":
            r["cavok"], r["vis_m"] = True, 10000
            encontro_algo = True
        elif _RE_VIS.match(t) and r["vis_m"] is None:
            v = int(_RE_VIS.match(t).group(1))
            r["vis_m"] = 10000 if v == 9999 else v
            encontro_algo = True
        elif t.endswith("SM") and r["vis_m"] is None:
            candidato = t
            if j > 0 and resto[j - 1].isdigit() and len(resto[j - 1]) <= 2:
                candidato = f"{resto[j - 1]} {t}"
            ms = _RE_VIS_SM.match(candidato)
            if ms:
                enteros = int(ms.group(1) or ms.group(4) or 0)
                frac = int(ms.group(2)) / int(ms.group(3)) if ms.group(2) else 0
                r["vis_m"] = int(round((enteros + frac) * _M_POR_MILLA))
                encontro_algo = True
        elif t.startswith("R") and "/" in t and len(t) > 4 and t[1:3].isdigit():
            pass  # alcance visual en pista: no se usa
        elif _RE_NUBE.match(t):
            m = _RE_NUBE.match(t)
            if m.group(2) != "///":
                altura = int(m.group(2)) * 100
                nubes.append(altura)
                if m.group(1) in ("BKN", "OVC"):
                    techos.append(altura)
        elif _RE_VV.match(t):
            m = _RE_VV.match(t)
            if m.group(1) != "///":
                techos.append(int(m.group(1)) * 100)
        elif t in _NUBES_SIN_TECHO:
            pass
        elif _RE_TEMP.match(t):
            m = _RE_TEMP.match(t)
            r["temp_c"], r["rocio_c"] = _temp(m.group(1)), _temp(m.group(2))
        elif _RE_QNH.match(t):
            r["qnh_hpa"] = float(_RE_QNH.match(t).group(1))
        elif _RE_ALT.match(t):
            r["qnh_hpa"] = round(int(_RE_ALT.match(t).group(1)) / 100 * 33.8639, 1)
        elif t.startswith("RE") or t.startswith("WS"):
            pass  # tiempo reciente / cizalladura: no son condiciones actuales
        else:
            m = _RE_FEN.match(t)
            if m and (m.group(2) or m.group(3)):
                fenomenos.append(t)
        j += 1

    for f in fenomenos:
        cerca = f.startswith("VC")
        r["fen_niebla"] |= int("FG" in f and not cerca)
        r["fen_neblina"] |= int("BR" in f)
        r["fen_bruma"] |= int("HZ" in f or "FU" in f)
        r["fen_lluvia"] |= int("RA" in f and not cerca)
        r["fen_llovizna"] |= int("DZ" in f)
        r["fen_chubasco"] |= int("SH" in f)
        r["fen_tormenta"] |= int("TS" in f)
        r["fen_nieve"] |= int("SN" in f or "SG" in f or "PL" in f)
    r["fenomenos"] = " ".join(fenomenos) or None
    r["techo_ft"] = min(techos) if techos else None
    r["nube_min_ft"] = min(nubes) if nubes else None
    if r["techo_ft"] is None and (r["cavok"] or nubes or any(t in _NUBES_SIN_TECHO for t in resto)):
        r["techo_ft"] = 99999  # sin techo: valor lógico alto
    r["categoria_vuelo"] = categoria_vuelo(r["techo_ft"], r["vis_m"])
    r["parseado_ok"] = bool(estacion_ok and encontro_algo)
    return r
