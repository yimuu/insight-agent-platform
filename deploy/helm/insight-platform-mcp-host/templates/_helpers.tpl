{{- define "insight-platform-mcp-host.name" -}}insight-platform-mcp-host{{- end }}
{{- define "insight-platform-mcp-host.labels" -}}
app.kubernetes.io/name: {{ include "insight-platform-mcp-host.name" . }}
app.kubernetes.io/component: mcp-host
app.kubernetes.io/part-of: insight-platform
{{- end }}
{{- define "insight-platform-mcp-host.image" -}}{{ printf "%s@%s" .Values.image.repository .Values.image.digest }}{{- end }}
