{{- define "insight-platform-security-egress.name" -}}
{{- printf "%s-%s" .Release.Name .Chart.Name | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "insight-platform-security-egress.labels" -}}
app.kubernetes.io/name: {{ .Chart.Name }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "insight-platform-security-egress.egressName" -}}
{{- printf "%s-egress" (include "insight-platform-security-egress.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "insight-platform-security-egress.authorityName" -}}
{{- printf "%s-security-authority" (include "insight-platform-security-egress.name" .) | trunc 63 | trimSuffix "-" -}}
{{- end }}

{{- define "insight-platform-security-egress.image" -}}
{{- printf "%s@%s" .Values.image.repository .Values.image.digest -}}
{{- end }}
