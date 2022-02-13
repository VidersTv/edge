package prometheus

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/viderstv/edge/src/configure"
)

type Instance interface {
	Register(r prometheus.Registerer)
}

type inst struct{}

func (m *inst) Register(r prometheus.Registerer) {
	r.MustRegister()
}

func LabelsFromKeyValue(kv []configure.KeyValue) prometheus.Labels {
	mp := prometheus.Labels{}

	for _, v := range kv {
		mp[v.Key] = v.Value
	}

	return mp
}

func New(opts SetupOptions) Instance {
	return &inst{}
}

type SetupOptions struct {
	Labels prometheus.Labels
}
