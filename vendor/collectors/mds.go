package collectors

import (
	"encoding/json"
	"log"
	"os/exec"
	"strings"
	"time"

	"github.com/prometheus/client_golang/prometheus"
)

const backgroundCollectInterval = time.Duration(5 * time.Minute)

const (
	MDSModeDisabled   = 0
	MDSModeForeground = 1
	MDSModeBackground = 2
)

func mdsGetStatus(config string) ([]string, error) {
	var (
		out []string
	)

	cmd := `ceph -c "%s" fs status 2>&1 | grep -A 2 Rank |tail -n 1 | awk '{print $4,$6}'`
	cmd = fmt.Sprintf(cmd, config)
	data, err := exec.Command("bash", "-c", cmd).Output()
	if err != nil {
		return nil, err
	}

	out = strings.Split(string(data), " ")
	return out, nil
}

type MDSCollector struct {
	config     string
	background bool

	// ceph mds daemon status
	MdsStatus *prometheus.GaugeVec

	getMDSStatus func(string) ([]string, error)
}

func NewMDSCollector(cluster string, config string, background bool) *MDSCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	mds := &MDSCollector{
		config:       config,
		background:   background,
		getMDSStatus: mdsGetStatus,

		MdsStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "mds_deamon_active",
				Help:        "MDS Deamon active stat",
				ConstLabels: labels,
			},
			[]string{"hostname"},
		),
	}

	if mds.background {
		go mds.backgroundCollect()
	}

	return mds
}

func (r *MDSCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		r.MdsStatus,
	}
}

func (r *MDSCollector) backgroundCollect() error {
	for {
		err := r.collect()
		if err != nil {
			log.Println("Failed to collect MDS stats", err)
		}
		time.Sleep(backgroundCollectInterval)
	}
}

func (r *MDSCollector) collect() error {
	data, err := r.getMDSStatus(r.config)
	if err != nil {
		return err
	}

	r.MdsStatus.WithLabelValues(data[1]).Set(float64(0))
	if data[0] == "active" {
		r.MdsStatus.WithLabelValues(data[1]).Set(float64(1))
	}

	return nil
}

func (r *MDSCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range r.collectorList() {
		metric.Describe(ch)
	}
}

func (r *MDSCollector) Collect(ch chan<- prometheus.Metric) {
	if !r.background {
		err := r.collect()
		if err != nil {
			log.Println("Failed to collect MDS stats", err)
		}
	}

	for _, metric := range r.collectorList() {
		metric.Collect(ch)
	}
}
