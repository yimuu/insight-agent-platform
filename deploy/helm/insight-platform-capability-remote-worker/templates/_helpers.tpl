{{- define "insight-platform-capability-remote-worker.name" -}}
insight-platform-capability-remote-worker
{{- end }}
{{- define "insight-platform-capability-remote-worker.labels" -}}
app.kubernetes.io/name: {{ include "insight-platform-capability-remote-worker.name" . }}
app.kubernetes.io/component: capability-remote-worker
app.kubernetes.io/part-of: insight-platform
{{- end }}
{{- define "insight-platform-capability-remote-worker.image" -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- end }}
