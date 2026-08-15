{{- define "insight-platform-model-worker.name" -}}
insight-platform-model-worker
{{- end }}

{{- define "insight-platform-model-worker.labels" -}}
app.kubernetes.io/name: {{ include "insight-platform-model-worker.name" . }}
app.kubernetes.io/component: model-worker
app.kubernetes.io/part-of: insight-platform
{{- end }}

{{- define "insight-platform-model-worker.image" -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- end }}
