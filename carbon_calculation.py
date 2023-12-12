# Imports
import csv
import sys
import datetime
import random
from typing import List, Optional, Tuple
import matplotlib.pyplot as plt
import matplotlib.dates as mdates


def _collect_caiso_data_1(fname):
    # kgCO2 per MWh
    with open(fname) as f:
        reader = csv.DictReader(f)
        return [{
                "Datetime": datetime.datetime.strptime(line["Datetime"], "%m/%d/%Y %H:%M"),
                "AvgCarbonIntensity": float(line["AvgCarbonIntensity"])
                } for line in reader]


def _collect_caiso_data_2(fname):
    # kgCO2 per MWh
    with open(fname) as f:
        reader = csv.DictReader(f)
        return [{
                "Datetime": datetime.datetime.strptime(line["Datetime"], "%Y-%m-%d %H:%M:%S"),
                "AvgCarbonIntensity": float(line["AvgCarbonIntensity"])
                } for line in reader]


def _read_jobs(fname):
    with open(fname) as f:
        reader = csv.DictReader(f)
        return [
            {**line, **{
                "server_utilization": float(line["server_utilization"]),
                "time": datetime.timedelta(hours=float(line["time"])),
            }} for line in reader
        ]


class HourlyAllocation:
    def __init__(self, energy=0.0):
        self.energy = energy
        self.jobs = []

    def __repr__(self) -> str:
        # return f"{self.energy}: {self.jobs}"
        return f"{self.energy} ({len(self.jobs)})"

    def __str__(self) -> str:
        return self.energy


class CloudScheduler:
    AVG_HARDWARE_USAGE_PER_YR_HRS = datetime.timedelta(hours=8760)

    # Hardware specs
    hardware_specs = {
        "xeon_e5_2670": {
            "cores": 10,
            "embodied_carbon": 2216.20,
            "tdp": 115,
            "tdp_coefficient": 0.25,
            "lifetime": AVG_HARDWARE_USAGE_PER_YR_HRS * 4.2,
        },
        "amd_epyc_7571": {
            "cores": 32,
            "embodied_carbon": 1610.40,
            "tdp": 120,  # Watts
            "tdp_coefficient": 0.3,
            "lifetime": AVG_HARDWARE_USAGE_PER_YR_HRS * 4.5,
        },
        "xeon_platinum_8176": {
            "cores": 28,
            "embodied_carbon": 35762.3,
            "tdp": 165,
            "tdp_coefficient": 0.3,
            "lifetime": AVG_HARDWARE_USAGE_PER_YR_HRS * 3.8,
        }
    }

    def __init__(self, intensity_data_fname: str, slot_algo: str):
        self.intensity_data = _collect_caiso_data_1(intensity_data_fname)
        self.slot_algo = slot_algo
        # Pair of (Amount of energy scheduled, list of jobs and time to run) for each hour of the day
        self.current_allocation = [HourlyAllocation() for i in range(24)]
        self.allocation_by_job = {}
        self.job_carbon = []

    @staticmethod
    def time_taken(job) -> datetime.timedelta:
        hw = CloudScheduler.hardware_specs[job["hardware"]]

        # How long will this job take? (in seconds)
        time_taken = datetime.timedelta(
            seconds=job["mflo"] / hw["mflo_per_sec"])
        return time_taken

    @staticmethod
    def embodied_carbon(job):
        hw = CloudScheduler.hardware_specs[job["hardware"]]

        # flops / total lifetime flops * total embodied carbon = job embodied carbon
        embodied = (
            job["time"] / hw["lifetime"]) * hw["embodied_carbon"]

        return embodied

    @staticmethod
    def energy_per_hr(job):
        time_required_hrs = job["time"] / datetime.timedelta(hours=1)
        energy_consumed = CloudScheduler.energy_consumed(job)
        return energy_consumed / time_required_hrs

    def energy_consumed(job):
        # Returns MWh
        hw = CloudScheduler.hardware_specs[job["hardware"]]

        en = job["server_utilization"] * (job["time"] / datetime.timedelta(hours=1)) * \
            hw["cores"] * hw["tdp"] * hw["tdp_coefficient"] / 1e6
        return en

    def submit_job(self, job) -> Optional[float]:
        # Start with embodied carbon
        embCarbon = CloudScheduler.embodied_carbon(job)

        # Optimize for operational carbon
        operCarbon = 0.0

        slots = self.get_best_slots(job)
        if len(slots) == 0:
            return None

        energy_per_hr = CloudScheduler.energy_per_hr(job)

        allocation = [0] * 24

        for (idx, intensity, time_usage) in slots:
            self.current_allocation[idx].energy += energy_per_hr * time_usage
            self.current_allocation[idx].jobs.append(
                {"id": job["id"], "time": time_usage})
            allocation[idx] += time_usage
            operCarbon += intensity * energy_per_hr * time_usage

        self.allocation_by_job[job["id"]] = allocation
        self.job_carbon.append(embCarbon + operCarbon)

        return embCarbon + operCarbon

    def get_best_slots(self, job) -> List[Tuple[int, float, float]]:
        if self.slot_algo == "simple":
            return self.get_best_slots_v1(job, False)
        elif self.slot_algo == "min_alloc":
            return self.get_best_slots_v1(job, True)
        # else ... other algorithms here!

    def get_best_slots_v1(self, job, dampen=False):
        after = [(idx, self.get_adjusted_intensity(idx) if dampen else self.get_intensity(idx)) for idx in range(24)]
        sorted_slots = sorted(after, key=lambda o: o[1])

        energy_per_hr = CloudScheduler.energy_per_hr(job)

        time_required_hrs = job["time"] / datetime.timedelta(hours=1)
        projected_carbon = CloudScheduler.embodied_carbon(job)
        success = True
        best_slots = []

        # Allocate job, compute overall carbon, return success
        for (idx, intensity) in sorted_slots:
            time_usage = min(1, time_required_hrs)

            # How much carbon would we get from this slot?
            projected_carbon += energy_per_hr * time_usage * intensity
            if projected_carbon > float(job["carbon_budget"]):
                success = False
                break

            best_slots.append((idx, intensity, time_usage))

            time_required_hrs -= 1
            if time_required_hrs <= 0:
                break

        return best_slots if success else []

    def get_intensity(self, idx):
        return self.intensity_data[idx]["AvgCarbonIntensity"]

    def get_adjusted_intensity(self, idx):
        return self.intensity_data[idx]["AvgCarbonIntensity"] + \
            200e6*self.current_allocation[idx].energy / \
            self.intensity_data[idx]["AvgCarbonIntensity"]

    def show_allocation(self, show=False):
        for i,x in enumerate(self.current_allocation):
            if x.energy == 0:
                continue
            job_descr = ", ".join(
                [f'job {j["id"]} for {j["time"]:.3f} hr(s)' for j in x.jobs])
            print(f"{i}:00\t{x.energy*(10**6):.3f}Wh\t{job_descr}")

        print("Job carbon:", self.job_carbon)

        fig, ax1 = plt.subplots()
        color = 'tab:blue'
        ax1.set_xlabel("hour of day (24hr)")
        ax1.set_ylabel("number of jobs allocated", color=color)
        ax1.bar(range(24), [len(ha.jobs)
                for ha in self.current_allocation], color=color, label='job count')
        ax1.tick_params(axis='y', labelcolor=color)

        ax2 = ax1.twinx()
        color = 'tab:green'
        ax2.set_ylabel("energy consumption (MWh)", color=color)
        ax2.plot(range(24), [
            ha.energy for ha in self.current_allocation], color=color, label='energy')
        ax2.tick_params(axis='y', labelcolor=color)

        fig.tight_layout()
        if show: plt.show()


# Jobs
RANDOM_JOBS = [
    {
        "id": i,
        "server_utilization": random.random() / 4.5 + 0.05,
        "time": datetime.timedelta(hours=random.random() * 2 + 0.5),
        "hardware": list(CloudScheduler.hardware_specs.keys())[int(random.random() * 3)],
        "carbon_budget": 2,
    }
    for i in range(10)
]


def main(alloc):
    scheduler = CloudScheduler("caiso-data/day_forecast_aci.csv", alloc)

    jobs = _read_jobs("sample_jobs.csv")

    for j in jobs:
        res = scheduler.submit_job(j)
        if res == None:
            print("Failed to allocate job", j["id"])
        else:
            print("Allocated job", j["id"], "with carbon", f"{res:.5f}")

    scheduler.show_allocation(False)


if __name__ == "__main__":
    main(sys.argv[1] if len(sys.argv) > 1 else "simple")
