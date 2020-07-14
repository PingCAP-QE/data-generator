package main

import (
	"sync"
	"time"

	"github.com/pingcap/log"
)

type SpeedControl struct {
	Rate  int64
	Token int64
	//MaxToken uint64
	Interval int64 // second
	Mu       sync.Mutex
}

func NewSpeedControl(rate, interval int64) *SpeedControl {
	s := &SpeedControl{
		Rate:     rate,
		Interval: interval,
	}
	go s.AwardToken()

	return s
}

func (f *SpeedControl) AwardToken() {
	timer := time.NewTicker(time.Duration(f.Interval) * time.Second)
	defer timer.Stop()
	for {
		select {
		case <-timer.C:
			f.Mu.Lock()
			f.Token += f.Rate * f.Interval
			f.Mu.Unlock()
		default:
		}
	}
}

func (f *SpeedControl) ApplyToken() (bool, int64) {
	f.Mu.Lock()
	defer f.Mu.Unlock()

	if f.Token >= 1 {
		num := f.Token
		f.Token = 0
		return true, num
	}

	return false, 0
}

func (f *SpeedControl) ApplyTokenSync() int64 {
	for {
		apply, num := f.ApplyToken()
		if apply {
			return num
		}

		log.S().Debugf("apply token failed, wait a second")
		time.Sleep(time.Second)
	}
}
