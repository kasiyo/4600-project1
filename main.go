package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"sort"
	"strconv"
	"strings"

	"github.com/olekukonko/tablewriter"
)

func main() {
	// CLI args
	f, closeFile, err := openProcessingFile(os.Args...)
	if err != nil {
		log.Fatal(err)
	}
	defer closeFile()

	// Load and parse processes
	processes, err := loadProcesses(f)
	if err != nil {
		log.Fatal(err)
	}

	// First-come, first-serve scheduling
	FCFSSchedule(os.Stdout, "First-come, first-serve", processes)

	// Shortest-job-first scheduling
	SJFSchedule(os.Stdout, "Shortest-job-first", processes)

	// Shortest-job-first, priority-scheduling
	SJFPrioritySchedule(os.Stdout, "Priority", processes)

	// Round-robin scheduling
	RRSchedule(os.Stdout, "Round-robin", processes)
}

func openProcessingFile(args ...string) (*os.File, func(), error) {
	if len(args) != 2 {
		return nil, nil, fmt.Errorf("%w: must give a scheduling file to process", ErrInvalidArgs)
	}
	// Read in CSV process CSV file
	f, err := os.Open(args[1])
	if err != nil {
		return nil, nil, fmt.Errorf("%v: error opening scheduling file", err)
	}
	closeFn := func() {
		if err := f.Close(); err != nil {
			log.Fatalf("%v: error closing scheduling file", err)
		}
	}

	return f, closeFn, nil
}

type (
	Process struct {
		ProcessID     int64
		ArrivalTime   int64
		BurstDuration int64
		Priority      int64

		startingTime int64
		isDone       bool
		hasMultiple  bool
		totalWait    int64
		stoppingTime int64
	}
	TimeSlice struct {
		PID   int64
		Start int64
		Stop  int64
	}
)

//region Schedulers

// FCFSSchedule outputs a schedule of processes in a GANTT chart and a table of timing given:
// • an output writer
// • a title for the chart
// • a slice of processes
func FCFSSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
	)
	for i := range processes {
		if processes[i].ArrivalTime > 0 {
			waitingTime = serviceTime - processes[i].ArrivalTime
		}
		totalWait += float64(waitingTime)

		start := waitingTime + processes[i].ArrivalTime

		turnaround := processes[i].BurstDuration + waitingTime
		totalTurnaround += float64(turnaround)

		completion := processes[i].BurstDuration + processes[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)

		schedule[i] = []string{
			fmt.Sprint(processes[i].ProcessID),
			fmt.Sprint(processes[i].Priority),
			fmt.Sprint(processes[i].BurstDuration),
			fmt.Sprint(processes[i].ArrivalTime),
			fmt.Sprint(waitingTime),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		}
		serviceTime += processes[i].BurstDuration

		gantt = append(gantt, TimeSlice{
			PID:   processes[i].ProcessID,
			Start: start,
			Stop:  serviceTime,
		})
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

func CheckPriority(arr []Process, index1, index2 int64) []Process {
	if arr[index1].Priority < arr[index2].Priority {
		//do nothing
	} else if arr[index1].Priority > arr[index2].Priority {
		if arr[index2].BurstDuration < arr[index1].BurstDuration {
			temp := arr[index1]
			arr[index1] = arr[index2]
			arr[index2] = temp
			return arr
		} else if arr[index2].BurstDuration > arr[index1].BurstDuration {
			//do nothing
		}
	}
	return arr

}

func tickUntilNextPriority(tick int64, p1, p2 Process) (int64, Process, Process) {
	if tick == p2.ArrivalTime {

		return tick, p1, p2
	} else if p1.BurstDuration == 0 && p1.isDone == false {
		p1.isDone = true
		p1.stoppingTime = tick
		return tickUntilNextPriority(tick+1, p1, p2)
	} else {
		p1.BurstDuration--
		return tickUntilNextPriority(tick+1, p1, p2)
	}
}

// Short-job-first, priority-scheduling function
func SJFPrioritySchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		waitingTime     int64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
		copyProc        []Process
		totalBurst      int64
		readyQueue      []Process
		priorityQueue   []Process
	)
	// sort by arrivalTime
	copyProc = append([]Process(nil), processes...)
	sort.SliceStable(copyProc, func(i, j int) bool {
		return copyProc[i].ArrivalTime < copyProc[j].ArrivalTime
	})
	//sort by priority
	priorityQueue = append(priorityQueue, processes...)
	sort.SliceStable(priorityQueue, func(i, j int) bool {
		return priorityQueue[i].Priority < priorityQueue[j].Priority
	})

	currBurst := 0
	//compare copyProc to priorityQueue
	for i, x := range copyProc {
		totalBurst += copyProc[i].BurstDuration
		Temp := copyProc[i]
		//check the next process if the copyProc and priorityQueue don't match up
		if i+1 < len(copyProc) && copyProc[i].ProcessID != priorityQueue[i].ProcessID {
			//loop to determine current burst until the process w higher priority arrives
			for int64(currBurst) < copyProc[i+1].ArrivalTime {
				copyProc[i].stoppingTime++
				currBurst++
			}
			//if currBurst is <= the total burstduration
			if currBurst < int(Temp.BurstDuration) {
				x.stoppingTime = int64(currBurst)
				x.BurstDuration = int64(currBurst)

				copyProc[i].BurstDuration -= int64(currBurst)
				readyQueue = append(readyQueue, copyProc[i])
			}
			//if the next copyProc matches the first ProcessID in priorityQueue
			if copyProc[i+1].ProcessID == priorityQueue[i].ProcessID {
				if copyProc[i].isDone == false {
					copyProc = append(copyProc[:i], append([]Process{x}, copyProc[i:]...)...)
				}
				copyProc[i] = x
			}
		}
		//now check next-next index after inserting part of the first process at the front
		//if next-next index has priority, swap it
		if i+2 < len(copyProc) && copyProc[i+1].Priority > copyProc[i+2].Priority {
			copyProc[i+1].totalWait = copyProc[i+2].BurstDuration
			x = copyProc[i+1]
			copyProc[i+1] = copyProc[i+2]

			if x.ProcessID == copyProc[i].ProcessID {
				x.isDone = true
			}
			copyProc[i+2] = x
			break
		}
	}

	totalBurst = 0
	for i := range processes {
		totalBurst += processes[i].BurstDuration
		burstCount := 0

		//total the burst of matching process ids
		for j := range copyProc {
			if copyProc[j].ProcessID == processes[i].ProcessID {
				burstCount += int(copyProc[j].BurstDuration)
				continue
			} else {
				continue
			}
		}
		//if all process ids add up to the original burstduration, last one in struct will be done
		if burstCount == int(processes[i].BurstDuration) {
			for k := len(copyProc) - 1; k > 0; k-- {
				if copyProc[k].ProcessID == processes[i].ProcessID {
					//set last matching process id to true
					copyProc[k].isDone = true
					break
				}
			}
		}
	}

	//build schedule/gantt
	for i := range copyProc {
		//set the waiting time for processes arriving after the first
		if copyProc[i].ArrivalTime > 0 {
			waitingTime = serviceTime - copyProc[i].ArrivalTime
			copyProc[i].totalWait = waitingTime
		}

		//set the waiting time for future processes of the same ProcessID
		if i+2 < len(copyProc) && copyProc[i+2].ProcessID == copyProc[i].ProcessID {
			copyProc[i+2].totalWait = copyProc[i+1].BurstDuration
		}

		//set the startingTiem and stoppingTime
		copyProc[i].startingTime = serviceTime
		copyProc[i].stoppingTime = serviceTime + copyProc[i].BurstDuration

		//sum up the totalWait and turnarounds for each process
		totalWait += float64(copyProc[i].totalWait)
		turnaround := copyProc[i].BurstDuration + copyProc[i].totalWait
		totalTurnaround += float64(turnaround)

		//calculate the completion for each process, increment serviceTime
		completion := copyProc[i].BurstDuration + copyProc[i].ArrivalTime + waitingTime
		lastCompletion = float64(completion)
		serviceTime += copyProc[i].BurstDuration

		schedule = append(schedule, []string{
			fmt.Sprint(copyProc[i].ProcessID),
			fmt.Sprint(copyProc[i].Priority),
			fmt.Sprint(copyProc[i].BurstDuration),
			fmt.Sprint(copyProc[i].ArrivalTime),
			fmt.Sprint(copyProc[i].totalWait),
			fmt.Sprint(turnaround),
			fmt.Sprint(copyProc[i].stoppingTime),
		})

		gantt = append(gantt, TimeSlice{
			PID:   copyProc[i].ProcessID,
			Start: copyProc[i].startingTime,
			Stop:  copyProc[i].stoppingTime,
		})

	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

// Shortest-job-first scheduling function
func SJFSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
		copyProc        []Process
		readyQueue      []Process
		currBurst       int64
		remBurst        int64
	)
	//sort by arrival time first
	sort.SliceStable(processes, func(i, j int) bool {
		if processes[i].ArrivalTime == processes[j].ArrivalTime {
			return processes[i].BurstDuration < processes[j].BurstDuration
		}
		return processes[i].ArrivalTime < processes[j].ArrivalTime
	})
	copyProc = append(copyProc, processes...)

	for i := range copyProc {
		if copyProc[i].ArrivalTime == 0 && copyProc[i].isDone != true {
			copyProc[i].totalWait = 0 //no wait
			copyProc[i].startingTime = copyProc[i].ArrivalTime
			copyProc[i].stoppingTime = copyProc[i].BurstDuration
			copyProc[i].isDone = true

			serviceTime += copyProc[i].BurstDuration
			readyQueue = append(readyQueue, copyProc[i])
		} else if i+1 < len(copyProc) && copyProc[i+1].BurstDuration < copyProc[i].BurstDuration {
			if copyProc[i].ArrivalTime < copyProc[i+1].ArrivalTime {
				//calculate current process burst duration before time for next one
				for j := serviceTime; j < copyProc[i+1].ArrivalTime; j++ {
					currBurst += 1
				}
				//save original burst duration as remaining burst
				remBurst = copyProc[i].BurstDuration

				copyProc[i].BurstDuration = currBurst
				copyProc[i].totalWait = serviceTime - copyProc[i].ArrivalTime
				copyProc[i].startingTime = serviceTime
				copyProc[i].stoppingTime = copyProc[i].BurstDuration + copyProc[i].ArrivalTime + copyProc[i].totalWait
				copyProc[i].isDone = false

				serviceTime += copyProc[i].BurstDuration
				readyQueue = append(readyQueue, copyProc[i])

				copyProc[i].BurstDuration = remBurst - currBurst

				//swap current process with next process
				copyProc[i], copyProc[i+1] = copyProc[i+1], copyProc[i]

				copyProc[i].totalWait = serviceTime - copyProc[i].ArrivalTime
				copyProc[i].startingTime = serviceTime
				copyProc[i].stoppingTime = serviceTime + copyProc[i].BurstDuration + copyProc[i].totalWait
				copyProc[i].isDone = true

				serviceTime += copyProc[i].BurstDuration
				readyQueue = append(readyQueue, copyProc[i])
			}
		} else if copyProc[i].isDone != true {
			for j := range readyQueue {
				if readyQueue[j].ProcessID == copyProc[i].ProcessID && readyQueue[j].isDone != true {
					copyProc[i].startingTime = serviceTime
					copyProc[i].stoppingTime = serviceTime + copyProc[i].BurstDuration
					copyProc[i].totalWait = copyProc[i].stoppingTime - copyProc[i].startingTime
					copyProc[i].isDone = true

					serviceTime += copyProc[i].BurstDuration
					readyQueue = append(readyQueue, copyProc[i])
				}
			}
		}
	}
	//iterate thru the readyQueue and schedule/gantt the processes
	for i := range readyQueue {
		turnaround := readyQueue[i].BurstDuration + readyQueue[i].totalWait
		completion := readyQueue[i].stoppingTime

		if readyQueue[i].isDone == true {
			totalTurnaround += float64(turnaround)
			totalWait += float64(readyQueue[i].totalWait)
		} else {
			for j := range readyQueue {
				if readyQueue[j].ProcessID == readyQueue[i].ProcessID {
					if readyQueue[j].isDone == true && readyQueue[i].isDone == false {
						totalWait -= float64(readyQueue[i].totalWait)
					}
				}
			}
		}

		schedule = append(schedule, []string{
			fmt.Sprint(readyQueue[i].ProcessID),
			fmt.Sprint(readyQueue[i].Priority),
			fmt.Sprint(readyQueue[i].BurstDuration),
			fmt.Sprint(readyQueue[i].ArrivalTime),
			fmt.Sprint(readyQueue[i].totalWait),
			fmt.Sprint(turnaround),
			fmt.Sprint(completion),
		})

		gantt = append(gantt, TimeSlice{
			PID:   readyQueue[i].ProcessID,
			Start: readyQueue[i].startingTime,
			Stop:  readyQueue[i].stoppingTime,
		})

		lastCompletion = float64(readyQueue[i].stoppingTime)
	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//Round-robin scheduling function
func RRSchedule(w io.Writer, title string, processes []Process) {
	var (
		serviceTime     int64
		totalWait       float64
		totalTurnaround float64
		lastCompletion  float64
		schedule        = make([][]string, len(processes))
		gantt           = make([]TimeSlice, 0)
		copyQueue       []Process
		timeQuantum     int64
		readyQueue      []Process
	)

	copyQueue = append([]Process(nil), processes...)
	//set timeQuantum to first process given
	timeQuantum = processes[0].BurstDuration
	//first find the lowest burst duration by looping thru processes
	for i := range processes {
		if timeQuantum > processes[i].BurstDuration {
			timeQuantum = processes[i].BurstDuration
		}
	}

	//build the readyQueue by splitting the elements and appending them back on
	for i, x := range copyQueue {
		copyQueue[i].startingTime = serviceTime
		//if BurstDuration == timeQuantum, it's done and has no multiples
		if copyQueue[i].BurstDuration == timeQuantum {
			x.isDone = true
			x.hasMultiple = false
			readyQueue = append(readyQueue, x)
			//else there are multiples and the current one is not done
		} else {
			copyQueue[i].isDone = false
			copyQueue[i].hasMultiple = true
			x.hasMultiple = true
			x.isDone = true

			x.BurstDuration = copyQueue[i].BurstDuration - timeQuantum
			copyQueue[i].BurstDuration -= x.BurstDuration

			readyQueue = append(readyQueue[:i], append([]Process{copyQueue[i]}, readyQueue[i:]...)...)
			readyQueue = append(readyQueue, x)
		}
	}

	//build schedule/gantt
	for i := range readyQueue {
		readyQueue[i].totalWait = serviceTime - readyQueue[i].ArrivalTime
		readyQueue[i].startingTime = serviceTime
		readyQueue[i].stoppingTime = serviceTime + readyQueue[i].BurstDuration

		//if current process is split into multiple, this is how the totalwait gets calculated
		if i-1 >= 0 && readyQueue[i].hasMultiple == true {
			//only the ones that are done get calculated here
			if readyQueue[i].isDone == true {
				readyQueue[i].totalWait = serviceTime - readyQueue[i-1].startingTime
			} else {
				readyQueue[i].totalWait = serviceTime - readyQueue[i].ArrivalTime
			}
		}
		//if the processID is done, sum it up
		if readyQueue[i].isDone == true {
			totalWait += float64(readyQueue[i].totalWait)
		}

		turnaround := readyQueue[i].BurstDuration + readyQueue[i].totalWait
		totalTurnaround += float64(turnaround)
		serviceTime += readyQueue[i].BurstDuration
		schedule = append(schedule, []string{
			fmt.Sprint(readyQueue[i].ProcessID),
			fmt.Sprint(readyQueue[i].Priority),
			fmt.Sprint(readyQueue[i].BurstDuration),
			fmt.Sprint(readyQueue[i].ArrivalTime),
			fmt.Sprint(readyQueue[i].totalWait),
			fmt.Sprint(turnaround),
			fmt.Sprint(readyQueue[i].stoppingTime),
		})

		gantt = append(gantt, TimeSlice{
			PID:   readyQueue[i].ProcessID,
			Start: readyQueue[i].startingTime,
			Stop:  readyQueue[i].stoppingTime,
		})
		lastCompletion = float64(readyQueue[i].stoppingTime)

	}

	count := float64(len(processes))
	aveWait := totalWait / count
	aveTurnaround := totalTurnaround / count
	aveThroughput := count / lastCompletion

	outputTitle(w, title)
	outputGantt(w, gantt)
	outputSchedule(w, schedule, aveWait, aveTurnaround, aveThroughput)
}

//endregion

//region Output helpers

func outputTitle(w io.Writer, title string) {
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
	_, _ = fmt.Fprintln(w, strings.Repeat(" ", len(title)/2), title)
	_, _ = fmt.Fprintln(w, strings.Repeat("-", len(title)*2))
}

func outputGantt(w io.Writer, gantt []TimeSlice) {
	_, _ = fmt.Fprintln(w, "Gantt schedule")
	_, _ = fmt.Fprint(w, "|")
	for i := range gantt {
		pid := fmt.Sprint(gantt[i].PID)
		padding := strings.Repeat(" ", (8-len(pid))/2)
		_, _ = fmt.Fprint(w, padding, pid, padding, "|")
	}
	_, _ = fmt.Fprintln(w)
	for i := range gantt {
		_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Start), "\t")
		if len(gantt)-1 == i {
			_, _ = fmt.Fprint(w, fmt.Sprint(gantt[i].Stop))
		}
	}
	_, _ = fmt.Fprintf(w, "\n\n")
}

func outputSchedule(w io.Writer, rows [][]string, wait, turnaround, throughput float64) {
	_, _ = fmt.Fprintln(w, "Schedule table")
	table := tablewriter.NewWriter(w)
	table.SetHeader([]string{"ID", "Priority", "Burst", "Arrival", "Wait", "Turnaround", "Exit"})
	table.AppendBulk(rows)
	table.SetFooter([]string{"", "", "", "",
		fmt.Sprintf("Average\n%.2f", wait),
		fmt.Sprintf("Average\n%.2f", turnaround),
		fmt.Sprintf("Throughput\n%.2f/t", throughput)})
	table.Render()
}

//endregion

//region Loading processes.

var ErrInvalidArgs = errors.New("invalid args")

func loadProcesses(r io.Reader) ([]Process, error) {
	rows, err := csv.NewReader(r).ReadAll()
	if err != nil {
		return nil, fmt.Errorf("%w: reading CSV", err)
	}

	processes := make([]Process, len(rows))
	for i := range rows {
		processes[i].ProcessID = mustStrToInt(rows[i][0])
		processes[i].BurstDuration = mustStrToInt(rows[i][1])
		processes[i].ArrivalTime = mustStrToInt(rows[i][2])
		if len(rows[i]) == 4 {
			processes[i].Priority = mustStrToInt(rows[i][3])
		}
	}

	return processes, nil
}

func mustStrToInt(s string) int64 {
	i, err := strconv.ParseInt(s, 10, 64)
	if err != nil {
		_, _ = fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	return i
}

//endregion
