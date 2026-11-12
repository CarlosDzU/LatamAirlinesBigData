# Metas de infraestructura. El Makefile principal las incluye con: -include infra/infra.mk
# Configuracion: AWS_PROFILE y AWS_REGION en el entorno o en .env (raiz del repo, ignorado por git).

-include .env
export AWS_PROFILE
AWS_REGION ?= us-east-1
export AWS_REGION
export AWS_DEFAULT_REGION := $(AWS_REGION)
PROYECTO ?= latam-ops
WORKERS ?= 0
# AUTO=-auto-approve para aplicar sin confirmacion interactiva
AUTO ?=

TF_BASE    := terraform -chdir=infra/base
TF_COMPUTE := terraform -chdir=infra/compute
base_out = $(shell $(TF_BASE) output -raw $(1) 2>/dev/null)

.PHONY: base-init base-plan base-apply base-destroy salt compute-init compute-plan compute-up compute-down \
        sync tunel history costo infra-validar

base-init: ## Inicializa el stack persistente (estado local)
	@echo ">> terraform init en infra/base"
	$(TF_BASE) init -input=false

base-plan: base-init ## Plan del stack persistente (S3, Glue, Athena, Budgets, SNS, IAM, Step Functions)
	@echo ">> terraform plan en infra/base"
	$(TF_BASE) plan -input=false -out=base.tfplan

base-apply: base-init ## Crea/actualiza el stack persistente (lake, catalogo, orquestacion, alertas)
	@echo ">> terraform apply en infra/base"
	$(TF_BASE) apply -input=false $(AUTO)

base-destroy: ## Solo al cierre del curso: requiere force_destroy=true en terraform.tfvars
	@echo ">> DESTRUYE el lake y todo el stack persistente"
	$(TF_BASE) destroy -input=false

salt: ## Crea la salt de seudonimizacion en SSM (fuera de Terraform)
	@echo ">> creando salt en SSM si no existe"
	scripts/crear_salt.sh

define VARS_COMPUTE
-var bucket_name=$(call base_out,bucket_name) \
-var crawler_name=$(call base_out,crawler_name) \
-var log_group_jobs=$(call base_out,log_group_jobs) \
-var log_group_ssm=$(call base_out,log_group_ssm) \
-var log_group_spark=$(call base_out,log_group_spark) \
-var sns_topic_arn=$(call base_out,sns_topic_arn) \
-var ruta_salt=$(call base_out,ruta_salt) \
-var region=$(AWS_REGION) \
-var proyecto=$(PROYECTO) \
-var worker_count=$(WORKERS)
endef

compute-init: ## Inicializa compute con backend S3 en el bucket del lake (lockfile, sin DynamoDB)
	@echo ">> terraform init en infra/compute (estado en s3://$(call base_out,bucket_name)/tfstate/compute/)"
	@test -n "$(call base_out,bucket_name)" || (echo "falta make base-apply"; exit 1)
	$(TF_COMPUTE) init -input=false -reconfigure \
	  -backend-config="bucket=$(call base_out,bucket_name)" \
	  -backend-config="key=tfstate/compute/terraform.tfstate" \
	  -backend-config="region=$(AWS_REGION)" \
	  -backend-config="use_lockfile=true" \
	  -backend-config="encrypt=true"

compute-plan: compute-init ## Plan del cluster efimero
	@echo ">> terraform plan en infra/compute (WORKERS=$(WORKERS))"
	$(TF_COMPUTE) plan -input=false $(VARS_COMPUTE)

compute-up: compute-init ## Levanta el cluster Spark: make compute-up WORKERS=0|1|2
	@echo ">> levantando cluster: 1 master + $(WORKERS) worker(s)"
	$(TF_COMPUTE) apply -input=false $(AUTO) $(VARS_COMPUTE)
	@echo ">> el arranque tarda ~5-8 min; progreso: make tunel o CloudWatch $(call base_out,log_group_spark)"

compute-down: compute-init ## Destruye el cluster (al terminar cada sesion). El lake no se toca.
	@echo ">> destruyendo cluster efimero (el bucket queda intacto)"
	$(TF_COMPUTE) destroy -input=false $(AUTO) $(VARS_COMPUTE)

sync: ## Publica src/, config/, contratos/, scripts/ en S3 y los instala en los nodos encendidos
	@echo ">> sincronizando codigo"
	scripts/sync_codigo.sh

tunel: ## Tuneles SSM: Jupyter 8888, Spark master 8080 y UI de la app 4040 (Ctrl+C cierra)
	@echo ">> abriendo tuneles SSM 8888, 8080, 4040"
	@scripts/tunel_ssm.sh 8080 8080 & scripts/tunel_ssm.sh 4040 4040 & scripts/tunel_ssm.sh 8888 8888; kill %1 %2 2>/dev/null || true

history: ## Tunel SSM al Spark History Server (http://localhost:18080)
	@echo ">> abriendo tunel al History Server"
	scripts/tunel_ssm.sh 18080 18080

costo: ## Costo del mes (total, por servicio y por etiqueta Project)
	@echo ">> consultando Cost Explorer"
	scripts/costo.sh

infra-validar: ## fmt -check + validate de ambos stacks (sin credenciales)
	@echo ">> validando Terraform"
	terraform fmt -check -recursive infra
	$(TF_BASE) init -backend=false -input=false >/dev/null && $(TF_BASE) validate
	$(TF_COMPUTE) init -backend=false -input=false >/dev/null && $(TF_COMPUTE) validate
