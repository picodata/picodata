{{/*
Expand the name of the chart.
*/}}
{{- define "picodata.name" -}}
{{- default $.Chart.Name $.Values.nameOverride | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Create a default fully qualified app name.
We truncate at 63 chars because some Kubernetes name fields are limited to this (by the DNS naming spec).
If release name contains chart name it will be used as a full name.
*/}}
{{- define "picodata.fullname" -}}
{{- if $.Values.fullnameOverride }}
{{- $.Values.fullnameOverride | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- $name := default $.Chart.Name $.Values.nameOverride }}
{{- if contains $name $.Release.Name }}
{{- $.Release.Name | trunc 63 | trimSuffix "-" }}
{{- else }}
{{- printf "%s-%s" $.Release.Name $name | trunc 63 | trimSuffix "-" }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Create chart name and version as used by the chart label.
*/}}
{{- define "picodata.chart" -}}
{{- printf "%s-%s" .Chart.Name .Chart.Version | replace "+" "_" | trunc 63 | trimSuffix "-" }}
{{- end }}

{{/*
Common labels
*/}}
{{- define "picodata.labels" -}}
helm.sh/chart: {{ include "picodata.chart" . }}
{{ include "picodata.selectorLabels" . }}
{{- if .Chart.AppVersion }}
app.kubernetes.io/version: {{ .Chart.AppVersion | quote }}
{{- end }}
app.kubernetes.io/managed-by: {{ .Release.Service }}
{{- end }}

{{/*
Selector labels
*/}}
{{- define "picodata.selectorLabels" -}}
app.kubernetes.io/name: {{ include "picodata.name" . }}
app.kubernetes.io/instance: {{ .Release.Name }}
{{- end }}

{{/*
Create the name of the service account to use
*/}}
{{- define "picodata.serviceAccountName" -}}
{{- if .Values.serviceAccount.create }}
{{- printf "%s-%s" (include "picodata.fullname" .) ( .Values.serviceAccount.name | default "picodata-serviceaccount") }}
{{- else }}
{{- default "default" .Values.serviceAccount.name }}
{{- end }}
{{- end }}

{{/*
Get router target binary port from values.
Should be used only with default function!
*/}}
{{- define "picodata.router.binaryTargetPort" -}}
{{- range .Values.router.service.ports }}
{{- if eq .name "binary" }}
{{- .targetPort }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Get router target http port from values.
Should be used only with default function!
*/}}
{{- define "picodata.router.httpTargetPort" -}}
{{- range .Values.router.service.ports }}
{{- if eq .name "http" }}
{{- .targetPort }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Get storage target binary port from values.
Should be used only with default function!
*/}}
{{- define "picodata.storage.binaryTargetPort" -}}
{{- range .Values.storage.service.ports }}
{{- if eq .name "binary" }}
{{- .targetPort }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Get router target http port from values.
Should be used only with default function!
*/}}
{{- define "picodata.storage.httpTargetPort" -}}
{{- range .Values.storage.service.ports }}
{{- if eq .name "http" }}
{{- .targetPort }}
{{- end }}
{{- end }}
{{- end }}

{{/*
Generate first peer hostname.
*/}}
{{- define "picodata.peerUri" -}}
{{ include "picodata.fullname" . }}-router-0.{{ include "picodata.fullname" . }}-router-interconnect.{{ .Release.namespace | default "default" }}.svc.cluster.local:{{ include "picodata.router.binaryTargetPort" . }}
{{- end -}}

{{/*
Generate storage uri.
*/}}
{{- define "picodata.storage.advertiseUri" -}}
$(INSTANCE_NAME).{{ include "picodata.fullname" . }}-storage-interconnect.{{ .Release.namespace | default "default" }}.svc.cluster.local:{{ include "picodata.storage.binaryTargetPort" . }}
{{- end -}}

{{/*
Generate router uri.
*/}}
{{- define "picodata.router.advertiseUri" -}}
$(INSTANCE_NAME).{{ include "picodata.fullname" . }}-router-interconnect.{{ .Release.namespace | default "default" }}.svc.cluster.local:{{ include "picodata.router.binaryTargetPort" . }}
{{- end -}}
