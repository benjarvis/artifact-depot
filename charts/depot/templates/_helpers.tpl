{{/*
Expand the name of the chart.
*/}}
{{- define "depot.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Create a default fully qualified app name.
*/}}
{{- define "depot.fullname" -}}
{{- if .Values.fullnameOverride -}}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- $name := default .Chart.Name .Values.nameOverride -}}
{{- if contains $name .Release.Name -}}
{{- .Release.Name | trunc 63 | trimSuffix "-" -}}
{{- else -}}
{{- printf "%s-%s" .Release.Name $name | trunc 63 | trimSuffix "-" -}}
{{- end -}}
{{- end -}}
{{- end -}}

{{/*
Chart label.
*/}}
{{- define "depot.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" -}}
{{- end -}}

{{/*
Common labels.
*/}}
{{- define "depot.labels" -}}
helm.sh/chart: {{ include "depot.chart" . }}
{{ include "depot.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end -}}

{{/*
Selector labels.
*/}}
{{- define "depot.selectorLabels" -}}
app.kubernetes.io/name: {{ include "depot.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end -}}

{{/*
Service account name.
*/}}
{{- define "depot.serviceAccountName" -}}
{{- if .Values.serviceAccount.create -}}
{{- default (include "depot.fullname" .) .Values.serviceAccount.name -}}
{{- else -}}
{{- default "default" .Values.serviceAccount.name -}}
{{- end -}}
{{- end -}}

{{/*
Image reference.
*/}}
{{- define "depot.image" -}}
{{- $tag := default .Chart.AppVersion .Values.image.tag -}}
{{- printf "%s:%s" .Values.image.repository $tag -}}
{{- end -}}

{{/*
Whether the workload should be a StatefulSet (redb) or a Deployment (dynamodb).
*/}}
{{- define "depot.isStateful" -}}
{{- eq .Values.kvStore.type "redb" -}}
{{- end -}}

{{/*
Name of the Secret that holds the rendered depotd.toml.
*/}}
{{- define "depot.configSecretName" -}}
{{- printf "%s-config" (include "depot.fullname" .) -}}
{{- end -}}

{{/*
Name of the Secret that holds AWS credentials (static only -- existingSecret is
used directly by name).
*/}}
{{- define "depot.awsSecretName" -}}
{{- printf "%s-aws" (include "depot.fullname" .) -}}
{{- end -}}

{{/*
Effective AWS Secret name (existingSecret override wins).
*/}}
{{- define "depot.awsSecretEffective" -}}
{{- if .Values.aws.existingSecret -}}
{{- .Values.aws.existingSecret -}}
{{- else -}}
{{- include "depot.awsSecretName" . -}}
{{- end -}}
{{- end -}}

{{/*
Name of the Secret that holds S3 blob-store credentials.
*/}}
{{- define "depot.s3SecretName" -}}
{{- printf "%s-s3" (include "depot.fullname" .) -}}
{{- end -}}

{{/*
Effective OTLP endpoint:
  - if observability.enabled, use the in-chart OTel Collector Service
  - else if tracing.otlpEndpoint is set, use it
  - else empty (no tracing)
*/}}
{{- define "depot.otlpEndpoint" -}}
{{- if .Values.observability.enabled -}}
http://{{ include "depot.fullname" . }}-otel-collector:4317
{{- else if .Values.tracing.otlpEndpoint -}}
{{- .Values.tracing.otlpEndpoint -}}
{{- else -}}
{{- end -}}
{{- end -}}
