package collectors

import (
	"log"
	"os/exec"
	"strings"
	"time"
        "fmt"
	"os"
	"bufio"
	"io/ioutil"
	"encoding/json"
	"strconv"

	"github.com/prometheus/client_golang/prometheus"
)

const (
	MDSModeDisabled   = 0
	MDSModeForeground = 1
	MDSModeBackground = 2
)

type OpenFiles struct {
	Stat     []Stat `json:"stat"`    
	Noparent string `json:"noparent"`
}

type Stat struct {
	Openfile string `json:"openfile"`
	Dirname  string `json:"dirname"` 
}

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
	
	str := strings.Trim(string(data), "\n")
  	out = strings.Split(str," ")
	return out, nil
}

func mdsGetOpenfiles(config string) (OpenFiles, error) {
	var openFiles OpenFiles
	checkupFile, err := os.Open(config)
  	if err != nil {
    		log.Printf("cannot open file: %s", err.Error())
    		return openFiles, err
  	}
  	defer checkupFile.Close()

  	checkupReader := bufio.NewReader(checkupFile)
  	checkupContent, _ := ioutil.ReadAll(checkupReader)

  	if err = json.Unmarshal(checkupContent, &openFiles); err != nil {
    		log.Printf("json unmarshal failed: %s", err.Error())
    		return openFiles, err
  	}
	
	return openFiles,nil
}

type MDSCollector struct {
	config     string
	background bool

	// ceph mds daemon status
	MdsStatus *prometheus.GaugeVec
	// add mds open file statistics
	MdsOpenFiles *prometheus.GaugeVec

	getMDSStatus func(string) ([]string, error)
	getMDSOpen   func(string) (OpenFiles, error)
}

func NewMDSCollector(cluster string, config string, background bool) *MDSCollector {
	labels := make(prometheus.Labels)
	labels["cluster"] = cluster
	mds := &MDSCollector{
		config:       config,
		background:   background,
		getMDSStatus: mdsGetStatus,
		getMDSOpen:   mdsGetOpenfiles,

		MdsStatus: prometheus.NewGaugeVec(
			prometheus.GaugeOpts{
				Namespace:   cephNamespace,
				Name:        "mds_deamon_active",
				Help:        "MDS Deamon active stat",
				ConstLabels: labels,
			},
			[]string{"hostname"},
		),
		MdsOpenFiles: prometheus.NewGaugeVec(
	                        prometheus.GaugeOpts{
                                Namespace:   cephNamespace,
                                Name:        "mds_open_files",
                                Help:        "MDS Deamon open files",
                                ConstLabels: labels,
                        },
                        []string{"dirname"},
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
		r.MdsOpenFiles,
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
		r.MdsStatus.WithLabelValues().Set(float64(0))
		return err
	}	

	if strings.Contains(data[0], "active") {
  		r.MdsStatus.WithLabelValues(data[1]).Set(float64(1))
	} else {
		r.MdsStatus.WithLabelValues(data[1]).Set(float64(0))
	}

	openFiles, err := r.getMDSOpen("/tmp/mds_openfiles_out")
	if err != nil {
		r.MdsOpenFiles.WithLabelValues().Set(float64(0))
		return err
	}

	
	for _, value := range openFiles.Stat {
		num, err := strconv.Atoi(value.Openfile)
		if err != nil {
			r.MdsOpenFiles.WithLabelValues(value.Dirname).Set(float64(0))
		} else {
			r.MdsOpenFiles.WithLabelValues(value.Dirname).Set(float64(num))
		}
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
