{{- define "insight-platform-registry-validation-worker.name" -}}
insight-platform-registry-validation-worker
{{- end -}}
{{- define "insight-platform-registry-validation-worker.labels" -}}
app.kubernetes.io/name: {{ include "insight-platform-registry-validation-worker.name" . }}
app.kubernetes.io/component: registry-validation-worker
{{- end -}}
{{- define "insight-platform-registry-validation-worker.image" -}}
{{ .Values.image.repository }}@{{ .Values.image.digest }}
{{- end -}}
