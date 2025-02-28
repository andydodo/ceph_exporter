package collectors

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"

	"strings"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	osdLabelFormat = "osd.%v"
)

const (
	scrubStateIdle          = 0
	scrubStateScrubbing     = 1
	scrubStateDeepScrubbing = 2
)

type cephPGDumpBriefResponse []struct {
	PGID          string `json:"pgid"`
	ActingPrimary int64  `json:"acting_primary"`
	Acting        []int  `json:"acting"`
	State         string `json:"state"`
}

// OSDCollector displays statistics about OSD in the ceph cluster.
// An important aspect of monitoring OSDs is to ensure that when the cluster is up and
// running that all OSDs that are in the cluster are up and running, too
type OSDCollector struct {
	conn Conn

	// osdScrubCache holds the cache of previous PG scrubs
	osdScrubCache map[int]int

	// CrushWeight is a persistent setting, and it affects how CRUSH assigns data to OSDs.
	// It displays the CRUSH weight for the OSD
	CrushWeight *prometheus.GaugeVec

	// Depth displays the OSD's level of hierarchy in the CRUSH map
	Depth *prometheus.GaugeVec

	// Reweight sets an override weight on the OSD.
	// It displays value within 0 to 1.
	Reweight *prometheus.GaugeVec

	// Bytes displays the total bytes available in the OSD
	Bytes *prometheus.GaugeVec

	// UsedBytes displays the total used bytes in the OSD
	UsedBytes *prometheus.GaugeVec

	// AvailBytes displays the total available bytes in the OSD
	AvailBytes *prometheus.GaugeVec

	// Utilization displays current utilization of the OSD
	Utilization *prometheus.GaugeVec

	// Variance displays current variance of the OSD from the standard utilization
	Variance *prometheus.GaugeVec

	// Pgs displays total no. of placement groups in the OSD.
	// Available in Ceph Jewel version.
	Pgs *prometheus.GaugeVec

	// CommitLatency displays in seconds how long it takes for an operation to be applied to disk
	CommitLatency *prometheus.GaugeVec

	// ApplyLatency displays in seconds how long it takes to get applied to the backing filesystem
	ApplyLatency *prometheus.GaugeVec

	// OSDIn displays the In state of the OSD
	OSDIn *prometheus.GaugeVec

	// OSDUp displays the Up state of the OSD
	OSDUp *prometheus.GaugeVec

	// OSDDownDesc displays OSDs present in the cluster in "down" state
	OSDDownDesc *prometheus.Desc

	// TotalBytes displays total bytes in all OSDs
	TotalBytes prometheus.Gauge

	// TotalUsedBytes displays total used bytes in all OSDs
	TotalUsedBytes prometheus.Gauge

	// TotalAvailBytes displays total available bytes in all OSDs
	TotalAvailBytes prometheus.Gauge

	// AverageUtil displays average utilization in all OSDs
	AverageUtil prometheus.Gauge

	// ScrubbingStateDesc depicts if an osd is being scrubbed
	// labelled by OSD
	ScrubbingStateDesc *prometheus.Desc
}

//NewOSDCollector creates an instance of the OSDCollector and instantiates
// the individual metrics that show information about the OSD.
func NewOSDCollector(conn Conn, cluster string) *OSDCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster

	return &OSDCollector{
		conn:          conn,
		osdScrubCache: make(map[int]int),

		CrushWeight: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_crush_weight",
				Help:        "OSD Crush Weight",
				ConstLabels: labels,
			},
			[]string{"osd"},
		),

		Depth: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_depth",
				Help:        "OSD Depth",
				ConstLabels: labels,
			},
			[]string{"osd"},
		),

		Reweight: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_reweight",
				Help:        "OSD Reweight",
				ConstLabels: labels,
			},
			[]string{"osd"},
		),

		Bytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_bytes",
				Help:        "OSD Total Bytes",
				ConstLabels: labels,
			},
			[]string{"osd"},
		),

		UsedBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_used_bytes",
				Help:        "OSD Used Storage in Bytes",
				ConstLabels: labels,
			},
			[]string{"osd"},
		),

		AvailBytes: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_avail_bytes",
				Help:        "OSD Available Storage in Bytes",
				ConstLabels: labels,
			},
			[]string{"osd"},
		),

		Utilization: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_utilization",
				Help:        "OSD Utilization",
				ConstLabels: labels,
			},
			[]string{"osd"},
		),

		Variance: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_variance",
				Help:        "OSD Variance",
				ConstLabels: labels,
			},
			[]string{"osd"},
		),

		Pgs: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_pgs",
				Help:        "OSD Placement Group Count",
				ConstLabels: labels,
			},
			[]string{"osd"},
		),

		TotalBytes: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_total_bytes",
				Help:        "OSD Total Storage Bytes",
				ConstLabels: labels,
			},
		),
		TotalUsedBytes: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_total_used_bytes",
				Help:        "OSD Total Used Storage Bytes",
				ConstLabels: labels,
			},
		),

		TotalAvailBytes: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_total_avail_bytes",
				Help:        "OSD Total Available Storage Bytes ",
				ConstLabels: labels,
			},
		),

		AverageUtil: prometheus.NewGauge(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_average_utilization",
				Help:        "OSD Average Utilization",
				ConstLabels: labels,
			},
		),

		CommitLatency: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_perf_commit_latency_seconds",
				Help:        "OSD Perf Commit Latency",
				ConstLabels: labels,
			},
			[]string{"osd"},
		),

		ApplyLatency: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_perf_apply_latency_seconds",
				Help:        "OSD Perf Apply Latency",
				ConstLabels: labels,
			},
			[]string{"osd"},
		),

		OSDIn: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_in",
				Help:        "OSD In Status",
				ConstLabels: labels,
			},
			[]string{"osd"},
		),

		OSDUp: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "osd_up",
				Help:        "OSD Up Status",
				ConstLabels: labels,
			},
			[]string{"osd"},
		),
		OSDDownDesc: prometheus.NewDesc(
			fmt.Sprintf("%s_osd_down", cephNamespace),
			"No. of OSDs down in the cluster",
			//Todo: use for find osd.0 -> hostname add by andy
			[]string{"hostname", "osd", "status"},
			labels,
		),
		ScrubbingStateDesc: prometheus.NewDesc(
			fmt.Sprintf("%s_osd_scrub_state", cephNamespace),
			"State of OSDs involved in a scrub",
			[]string{"osd"},
			labels,
		),
	}
}

func (o *OSDCollector) collectorList() []prometheus.Collector {
	return []prometheus.Collector{
		o.CrushWeight,
		o.Depth,
		o.Reweight,
		o.Bytes,
		o.UsedBytes,
		o.AvailBytes,
		o.Utilization,
		o.Variance,
		o.Pgs,
		o.TotalBytes,
		o.TotalUsedBytes,
		o.TotalAvailBytes,
		o.AverageUtil,
		o.CommitLatency,
		o.ApplyLatency,
		o.OSDIn,
		o.OSDUp,
	}
}

type cephOSDDF struct {
	OSDNodes []struct {
		Name        string      `json:"name"`
		CrushWeight json.Number `json:"crush_weight"`
		Depth       json.Number `json:"depth"`
		Reweight    json.Number `json:"reweight"`
		KB          json.Number `json:"kb"`
		UsedKB      json.Number `json:"kb_used"`
		AvailKB     json.Number `json:"kb_avail"`
		Utilization json.Number `json:"utilization"`
		Variance    json.Number `json:"var"`
		Pgs         json.Number `json:"pgs"`
	} `json:"nodes"`

	Summary struct {
		TotalKB      json.Number `json:"total_kb"`
		TotalUsedKB  json.Number `json:"total_kb_used"`
		TotalAvailKB json.Number `json:"total_kb_avail"`
		AverageUtil  json.Number `json:"average_utilization"`
	} `json:"summary"`
}

type cephPerfStat struct {
	PerfInfo []struct {
		ID    json.Number `json:"id"`
		Stats struct {
			CommitLatency json.Number `json:"commit_latency_ms"`
			ApplyLatency  json.Number `json:"apply_latency_ms"`
		} `json:"perf_stats"`
	} `json:"osd_perf_infos"`
}

type cephOSDDump struct {
	OSDs []struct {
		OSD json.Number `json:"osd"`
		Up  json.Number `json:"up"`
		In  json.Number `json:"in"`
	} `json:"osds"`
}

type cephOSDTreeDown struct {
	Nodes []struct {
		ID     int64  `json:"id"`
		Name   string `json:"name"`
		Type   string `json:"type"`
		Status string `json:"status"`
	} `json:"nodes"`
	Stray []struct {
		ID     int64  `json:"id"`
		Name   string `json:"name"`
		Type   string `json:"type"`
		Status string `json:"status"`
	} `json:"stray"`
}

//Todo: add by andy
type cephOSDFindName struct {
	Osd           int64         `json:"osd"`
	IP            string        `json:"ip"`
	OsdFsid       string        `json:"osd_fsid"`
	CrushLocation CrushLocation `json:"crush_location"`
}

type CrushLocation struct {
	Host string `json:"host"`
	Root string `json:"root"`
}

func (o *OSDCollector) collectOSDDF() error {
	cmd := o.cephOSDDFCommand()

	buf, _, err := o.conn.MonCommand(cmd)
	if err != nil {
		log.Println("[ERROR] Unable to collect data from ceph osd df", err)
		return err
	}

	// Workaround for Ceph Jewel after 10.2.5 produces invalid json when osd is out
	buf = bytes.Replace(buf, []byte("-nan"), []byte("0"), -1)

	osdDF := &cephOSDDF{}
	if err := json.Unmarshal(buf, osdDF); err != nil {
		return err
	}

	for _, node := range osdDF.OSDNodes {

		crushWeight, err := node.CrushWeight.Float64()
		if err != nil {
			return err
		}

		o.CrushWeight.WithLabelValues(node.Name).Set(crushWeight)

		depth, err := node.Depth.Float64()
		if err != nil {

			return err
		}

		o.Depth.WithLabelValues(node.Name).Set(depth)

		reweight, err := node.Reweight.Float64()
		if err != nil {
			return err
		}

		o.Reweight.WithLabelValues(node.Name).Set(reweight)

		osdKB, err := node.KB.Float64()
		if err != nil {
			return nil
		}

		o.Bytes.WithLabelValues(node.Name).Set(osdKB * 1e3)

		usedKB, err := node.UsedKB.Float64()
		if err != nil {
			return err
		}

		o.UsedBytes.WithLabelValues(node.Name).Set(usedKB * 1e3)

		availKB, err := node.AvailKB.Float64()
		if err != nil {
			return err
		}

		o.AvailBytes.WithLabelValues(node.Name).Set(availKB * 1e3)

		util, err := node.Utilization.Float64()
		if err != nil {
			return err
		}

		o.Utilization.WithLabelValues(node.Name).Set(util)

		variance, err := node.Variance.Float64()
		if err != nil {
			return err
		}

		o.Variance.WithLabelValues(node.Name).Set(variance)

		pgs, err := node.Pgs.Float64()
		if err != nil {
			continue
		}

		o.Pgs.WithLabelValues(node.Name).Set(pgs)

	}

	totalKB, err := osdDF.Summary.TotalKB.Float64()
	if err != nil {
		return err
	}

	o.TotalBytes.Set(totalKB * 1e3)

	totalUsedKB, err := osdDF.Summary.TotalUsedKB.Float64()
	if err != nil {
		return err
	}

	o.TotalUsedBytes.Set(totalUsedKB * 1e3)

	totalAvailKB, err := osdDF.Summary.TotalAvailKB.Float64()
	if err != nil {
		return err
	}

	o.TotalAvailBytes.Set(totalAvailKB * 1e3)

	averageUtil, err := osdDF.Summary.AverageUtil.Float64()
	if err != nil {
		return err
	}

	o.AverageUtil.Set(averageUtil)

	return nil

}

func (o *OSDCollector) collectOSDPerf() error {
	osdPerfCmd := o.cephOSDPerfCommand()
	buf, _, err := o.conn.MonCommand(osdPerfCmd)
	if err != nil {
		log.Println("[ERROR] Unable to collect data from ceph osd perf", err)
		return err
	}

	osdPerf := &cephPerfStat{}
	if err := json.Unmarshal(buf, osdPerf); err != nil {
		return err
	}

	for _, perfStat := range osdPerf.PerfInfo {
		osdID, err := perfStat.ID.Int64()
		if err != nil {
			return err
		}
		osdName := fmt.Sprintf(osdLabelFormat, osdID)

		commitLatency, err := perfStat.Stats.CommitLatency.Float64()
		if err != nil {
			return err
		}
		o.CommitLatency.WithLabelValues(osdName).Set(commitLatency / 1e3)

		applyLatency, err := perfStat.Stats.ApplyLatency.Float64()
		if err != nil {
			return err
		}
		o.ApplyLatency.WithLabelValues(osdName).Set(applyLatency / 1e3)
	}

	return nil
}

func (o *OSDCollector) collectOSDTreeDown(ch chan<- prometheus.Metric) error {
	osdDownCmd := o.cephOSDTreeCommand("down")
	buff, _, err := o.conn.MonCommand(osdDownCmd)
	if err != nil {
		log.Println("[ERROR] Unable to collect data from ceph osd tree down", err)
		return err
	}

	osdDown := &cephOSDTreeDown{}
	if err := json.Unmarshal(buff, osdDown); err != nil {
		return err
	}

	downItems := append(osdDown.Nodes, osdDown.Stray...)

	for _, downItem := range downItems {
		if downItem.Type != "osd" {
			continue
		}

		//
		osdFindCmd := o.cephOSDFindCommand(downItem.ID)
		datas, _, err := o.conn.MonCommand(osdFindCmd)
		if err != nil {
			log.Println("[ERROR] Unable to collect data from ceph osd find id", err)
			return err
		}
		osdFind := &cephOSDFindName{}
		if err := json.Unmarshal(datas, osdFind); err != nil {
			return err
		}
		hostName := strings.Split(osdFind.IP, ":")

		osdName := downItem.Name

		ch <- prometheus.MustNewConstMetric(o.OSDDownDesc, prometheus.GaugeValue, 1, hostName[0], osdName, downItem.Status)
	}

	return nil
}

func (o *OSDCollector) collectOSDDump() error {
	osdDumpCmd := o.cephOSDDump()
	buff, _, err := o.conn.MonCommand(osdDumpCmd)
	if err != nil {
		log.Println("[ERROR] Unable to collect data from ceph osd dump", err)
		return err
	}

	osdDump := &cephOSDDump{}
	if err := json.Unmarshal(buff, osdDump); err != nil {
		return err
	}

	for _, dumpInfo := range osdDump.OSDs {
		osdID, err := dumpInfo.OSD.Int64()
		if err != nil {
			return err
		}
		osdName := fmt.Sprintf(osdLabelFormat, osdID)

		in, err := dumpInfo.In.Float64()
		if err != nil {
			return err
		}

		o.OSDIn.WithLabelValues(osdName).Set(in)

		up, err := dumpInfo.Up.Float64()
		if err != nil {
			return err
		}

		o.OSDUp.WithLabelValues(osdName).Set(up)
	}

	return nil

}

func (o *OSDCollector) collectOSDScrubState(ch chan<- prometheus.Metric) error {
	cmd := o.cephPGDumpCommand()
	buf, _, err := o.conn.MonCommand(cmd)
	if err != nil {
		return err
	}

	stats := cephPGDumpBriefResponse{}
	if err := json.Unmarshal(buf, &stats); err != nil {
		return err
	}

	// need to reset the PG scrub state since the scrub might have ended within the last prom scrape interval.
	//  This forces us to report scrub state on all previously discovered osds
	// We may be able to remove the "cache" when using prometheus 2.0 if we can tune how
	// unreported/abandoned gauges are treated (ie set to 0).
	for i := range o.osdScrubCache {
		o.osdScrubCache[i] = scrubStateIdle
	}

	for _, pg := range stats {
		if strings.Contains(pg.State, "scrubbing") {
			scrubState := scrubStateScrubbing
			if strings.Contains(pg.State, "deep") {
				scrubState = scrubStateDeepScrubbing
			}

			for _, osd := range pg.Acting {
				o.osdScrubCache[osd] = scrubState
			}
		}
	}

	for i, v := range o.osdScrubCache {
		ch <- prometheus.MustNewConstMetric(
			o.ScrubbingStateDesc,
			prometheus.GaugeValue,
			float64(v),
			fmt.Sprintf(osdLabelFormat, i))
	}

	return nil
}

func (o *OSDCollector) cephOSDDump() []byte {
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "osd dump",
		"format": "json",
	})
	if err != nil {
		panic(err)
	}
	return cmd
}

func (o *OSDCollector) cephOSDDFCommand() []byte {
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "osd df",
		"format": "json",
	})
	if err != nil {
		panic(err)
	}
	return cmd
}

func (o *OSDCollector) cephOSDPerfCommand() []byte {
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "osd perf",
		"format": "json",
	})
	if err != nil {
		panic(err)
	}
	return cmd
}

func (o *OSDCollector) cephOSDTreeCommand(states ...string) []byte {
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "osd tree",
		"states": states,
		"format": "json",
	})
	if err != nil {
		panic(err)
	}
	return cmd
}

//Todo: use for find osd.0 -> hostname add by andy
func (o *OSDCollector) cephOSDFindCommand(id int64) []byte {
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix": "osd find",
		"id":     id,
		"format": "json",
	})
	if err != nil {
		panic(err)
	}
	return cmd
}

func (o *OSDCollector) cephPGDumpCommand() []byte {
	cmd, err := json.Marshal(map[string]interface{}{
		"prefix":       "pg dump",
		"dumpcontents": []string{"pgs_brief"},
		"format":       jsonFormat,
	})
	if err != nil {
		// panic! because ideally in no world this hard-coded input
		// should fail.
		panic(err)
	}
	return cmd
}

// Describe sends the descriptors of each OSDCollector related metrics we have defined
// to the provided prometheus channel.
func (o *OSDCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, metric := range o.collectorList() {
		metric.Describe(ch)
	}
	ch <- o.ScrubbingStateDesc
}

// Collect sends all the collected metrics to the provided prometheus channel.
// It requires the caller to handle synchronization.
func (o *OSDCollector) Collect(ch chan<- prometheus.Metric) {

	// Reset daemon specifc metrics; daemons can leave the cluster
	o.CrushWeight.Reset()
	o.Depth.Reset()
	o.Reweight.Reset()
	o.Bytes.Reset()
	o.UsedBytes.Reset()
	o.AvailBytes.Reset()
	o.Utilization.Reset()
	o.Variance.Reset()
	o.Pgs.Reset()
	o.CommitLatency.Reset()
	o.ApplyLatency.Reset()
	o.OSDIn.Reset()
	o.OSDUp.Reset()

	if err := o.collectOSDPerf(); err != nil {
		log.Println("failed collecting osd perf stats:", err)
	}

	if err := o.collectOSDDump(); err != nil {
		log.Println("failed collecting osd dump:", err)
	}

	if err := o.collectOSDDF(); err != nil {
		log.Println("failed collecting osd metrics:", err)
	}

	if err := o.collectOSDTreeDown(ch); err != nil {
		log.Println("failed collecting osd metrics:", err)
	}

	for _, metric := range o.collectorList() {
		metric.Collect(ch)
	}

	if err := o.collectOSDScrubState(ch); err != nil {
		log.Println("failed collecting osd scrub state:", err)
	}
}
