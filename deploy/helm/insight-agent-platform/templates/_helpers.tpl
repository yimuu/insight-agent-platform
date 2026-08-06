{{- define "insight-agent-platform.name" -}}
{{- default .Chart.Name .Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "insight-agent-platform.fullname" -}}
{{- if .Values.fullnameOverride }}
{{- .Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" .Release.Name (include "insight-agent-platform.name" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}

{{- define "insight-agent-platform.labels" -}}
helm.sh/chart: {{ printf "%s-%s" .Chart.Name .Chart.Version | quote }}
app.kubernetes.io/name: {{ include "insight-agent-platform.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{- define "insight-agent-platform.selectorLabels" -}}
app.kubernetes.io/name: {{ include "insight-agent-platform.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
app.kubernetes.io/component: runtime
{{- end }}

{{- define "insight-agent-platform.postgresqlName" -}}
{{- printf "%s-postgresql" (include "insight-agent-platform.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}

{{- define "insight-agent-platform.databaseSecretName" -}}
{{- if .Values.externalDatabase.existingSecret }}
{{- .Values.externalDatabase.existingSecret }}
{{- else }}
{{- printf "%s-database" (include "insight-agent-platform.fullname" .) | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
