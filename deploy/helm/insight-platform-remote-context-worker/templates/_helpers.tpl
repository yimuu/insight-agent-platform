{{- define "insight-platform-remote-context-worker.name" -}}insight-platform-remote-context-worker{{- end }}
{{- define "insight-platform-remote-context-worker.labels" -}}
app.kubernetes.io/name: {{ include "insight-platform-remote-context-worker.name" . }}
app.kubernetes.io/component: context-worker
app.kubernetes.io/part-of: insight-platform
{{- end }}
{{- define "insight-platform-remote-context-worker.image" -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- end }}
