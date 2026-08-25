{{- define "insight-platform-orchestration-worker.name" -}}
insight-platform-orchestration-worker
{{- end }}

{{- define "insight-platform-orchestration-worker.labels" -}}
app.kubernetes.io/name: {{ include "insight-platform-orchestration-worker.name" . }}
app.kubernetes.io/component: orchestration-worker
app.kubernetes.io/part-of: insight-platform
{{- end }}

{{- define "insight-platform-orchestration-worker.image" -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- end }}
