{{- define "insight-platform-artifact-broker.name" -}}
insight-platform-artifact-broker
{{- end }}

{{- define "insight-platform-artifact-broker.labels" -}}
app.kubernetes.io/name: {{ include "insight-platform-artifact-broker.name" . }}
app.kubernetes.io/component: artifact-broker
app.kubernetes.io/part-of: insight-platform
{{- end }}

{{- define "insight-platform-artifact-broker.image" -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- end }}
