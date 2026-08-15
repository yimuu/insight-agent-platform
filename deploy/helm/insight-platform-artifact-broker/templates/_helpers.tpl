{{- define "insight-platform-artifact-broker.name" -}}
{{- printf "insight-platform-artifact-broker-%s" .audience -}}
{{- end }}

{{- define "insight-platform-artifact-broker.labels" -}}
app.kubernetes.io/name: insight-platform-artifact-broker
app.kubernetes.io/component: {{ printf "artifact-broker-%s" .audience }}
app.kubernetes.io/part-of: insight-platform
insight.platform/workload-role: {{ printf "artifact-broker-%s" .audience }}
{{- end }}

{{- define "insight-platform-artifact-broker.image" -}}
{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}
{{- end }}
