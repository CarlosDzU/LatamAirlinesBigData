"""Lanzador para spark-submit: delega en latam_ops.orquestacion.ejecutar_paso con los mismos argumentos."""

import sys

from latam_ops.orquestacion.ejecutar_paso import main

if __name__ == "__main__":
    main(sys.argv[1:])
