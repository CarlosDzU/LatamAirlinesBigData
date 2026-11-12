"""latam_ops: alerta temprana T-2 h de demoras de salida de LATAM Airlines.

Fuentes: ANAC VRA (vuelos) y METAR crudo del archivo IEM (clima, texto no estructurado).
Arquitectura: data lake bronze/silver/gold en S3, procesamiento con Spark 3.5.
"""

__version__ = "1.0.0"
