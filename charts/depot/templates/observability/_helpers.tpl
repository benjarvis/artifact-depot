{{/*
Common labels for observability components. Usage:
  {{ include "depot.obs.labels" (list . "minio") | nindent 4 }}
where the second element is the component name.
*/}}
{{- define "depot.obs.labels" -}}
{{- $ctx := index . 0 -}}
{{- $component := index . 1 -}}
helm.sh/chart: {{ include "depot.chart" $ctx }}
app.kubernetes.io/name: {{ $component }}
app.kubernetes.io/instance: {{ $ctx.Release.Name }}
app.kubernetes.io/component: {{ $component }}
app.kubernetes.io/part-of: {{ include "depot.name" $ctx }}
app.kubernetes.io/managed-by: {{ $ctx.Release.Service }}
{{- end -}}

{{- define "depot.obs.selectorLabels" -}}
{{- $ctx := index . 0 -}}
{{- $component := index . 1 -}}
app.kubernetes.io/name: {{ $component }}
app.kubernetes.io/instance: {{ $ctx.Release.Name }}
app.kubernetes.io/component: {{ $component }}
{{- end -}}
