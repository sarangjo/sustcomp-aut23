# Imports
import csv
import sys
import datetime
import random
import math
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
        "xeon_platinum_8124": {
            "cores": 18,
            "clock_speed": 3,  # GHz
            "mflo_per_sec": 66433,  # MOps/sec
            # TODO needs adjusting, this number represents Server-Side Java but we are trying to be a bit more general.
            # It's also using the numbers for Xeon Platinum 8180 from https://www.spec.org/power_ssj2008/results/power_ssj2008.html
            "watts_per_mflop": .03,
            "embodied_carbon": 1344.1,  # kgCO2eq
            "lifetime": AVG_HARDWARE_USAGE_PER_YR_HRS * 5,
        },
        "amd_epyc_7571": {
            "cores": 32,
            "embodied_carbon": 1610.40,
            "tdp": 120,  # Watts
            "tdp_coefficient": 0.3,
            "lifetime": AVG_HARDWARE_USAGE_PER_YR_HRS * 4.5,
        },
    }

    # AVG_HARDWARE_LIFE_YRS = random.randint(2, 7)

    def __init__(self, intensity_data_fname: str):
        self.intensity_data = _collect_caiso_data_1(intensity_data_fname)
        # Pair of (Amount of energy scheduled, list of jobs and time to run) for each hour of the day
        self.current_allocation = [HourlyAllocation() for i in range(24)]
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
        """
        Okay! Now let's just simply allocate with no brains. We need to specify some sort of capacity to
        limit how many jobs can all be allocated to the lowest emissions.

        Returns allocation and total carbon
        """
        # Start with embodied carbon
        embCarbon = CloudScheduler.embodied_carbon(job)

        # Optimize for operational carbon
        operCarbon = 0.0

        slots = self.get_best_slots(job, "simple")
        if len(slots) == 0:
            return None

        energy_per_hr = CloudScheduler.energy_per_hr(job)

        for (idx, intensity, time_usage) in slots:
            self.current_allocation[idx].energy += energy_per_hr * time_usage
            self.current_allocation[idx].jobs.append(
                {"id": job["id"], "time": time_usage})
            operCarbon += intensity * energy_per_hr * time_usage

        self.job_carbon.append(embCarbon + operCarbon)

        return embCarbon + operCarbon

        # Step 2: get best slots
        slots = self.get_best_slots_v1(method="dampened")[:num_slots]

        # Precompute the total carbon this "best allocation" would take
        for (idx, intensity) in slots[:full_slots]:
            carbon += energy_per_hr
        if num_slots != full_slots:
            (idx, intensity) = slots[full_slots]
            carbon += intensity * energy_per_hr * (time_required_hrs % 1)

        # Is this possible?
        if carbon > float(job["carbon_budget"]):
            return False

    def get_best_slots(self, job, method) -> List[Tuple[int, float, float]]:
        if method == "simple":
            return self.get_best_slots_v1(job, False)
        elif method == "min_alloc":
            return self.get_best_slots_v1(job, True)
        elif method == "max_under":
            return self.get_best_slots_max_under(job)

    def get_best_slots_v1(self, job, dampen=False):
        """
        Simply by intensity (which is sorted) and curbing by capacity
        """
        after = [(idx, self.get_adjusted_intensity(idx) if dampen else self.get_intensity(idx)) for idx in range(24)]
        sorted_slots = sorted(after, key=lambda o: o[1])

        energy_per_hr = CloudScheduler.energy_per_hr(job)

        time_required_hrs = job["time"] / datetime.timedelta(hours=1)
        projected_carbon = CloudScheduler.embodied_carbon(job)
        success = True
        best_slots = []

        # Allocate job, compute overall carbon, return success
        # Whole hours
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

    def get_best_slots_max_under(self, job):
        pass

    def get_intensity(self, idx):
        return self.intensity_data[idx]["AvgCarbonIntensity"]

    def get_adjusted_intensity(self, idx):
        return self.intensity_data[idx]["AvgCarbonIntensity"] + \
            3.5e6*self.current_allocation[idx].energy / \
            self.intensity_data[idx]["AvgCarbonIntensity"]

    def show_allocation(self, save=False):
        for x in self.current_allocation:
            job_descr = ", ".join(
                [f'job {j["id"]} for {j["time"]:.3f} hr(s)' for j in x.jobs])
            print(f"{x.energy*(10**6):.3f}\t{job_descr}")

        print(self.job_carbon)

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
        plt.show()


# Jobs
SAMPLE_JOB = {
    # "mflo": 30e6,  # Play around with this to find a reasonable # of megaflops
    "id": 1,
    "server_utilization": .32,
    "time": datetime.timedelta(hours=1.5),  # hours
    "hardware": "amd_epyc_7571"
}
JOBS = [
    {
        "id": i,
        "server_utilization": random.random() / 4.5 + 0.2,
        "time": datetime.timedelta(hours=random.random() * 2 + 0.5),
        "hardware": "amd_epyc_7571",
    }
    for i in range(10)
]


def main():
    scheduler = CloudScheduler("caiso-data/day_forecast_aci.csv")

    jobs = _read_jobs("sample_jobs.csv")

    for j in jobs:
        res = scheduler.submit_job(j)
        if res == None:
            print("Failed to allocate job", j["id"])
        else:
            print("Allocated job", j["id"], "with carbon", res)

    scheduler.show_allocation()


def jan_caiso():
    data = _collect_caiso_data_1("full_caiso.csv")

    plt.plot([d["Datetime"] for d in data], [
             d["AvgCarbonIntensity"] for d in data])
    plt.xlabel("Date")
    plt.ylabel("Average Carbon Intensity (kgCO2/MWh)")
    plt.title("CAISO Historical Data (January 2022)")
    plt.show()


def year_caiso_lowest_intensity():
    data = _collect_caiso_data_2("caiso-data/CAISO_aci_2022.csv")

    # Go day by day
    filtered_data = []
    for i in range(0, len(data), 24):
        filtered_data.append(
            min(data[i:i+24], key=lambda d: d["AvgCarbonIntensity"]))

    plt.plot([d["Datetime"] for d in data], [
             d["AvgCarbonIntensity"] for d in data])
    plt.xlabel("Date")
    plt.ylabel("Average Carbon Intensity (kgCO2/MWh)")
    plt.title("CAISO Lowest ACI By Day (2022)")
    plt.savefig("CAISO_Lowest_Intensity_By_Day.png")
    plt.show()


def day_caiso_intensity():
    # Pick my favorite day
    day_index = 190

    data = _collect_caiso_data_2(
        "caiso-data/CAISO_aci_2022.csv")[day_index*24:(day_index+1)*24]

    plt.plot([d["Datetime"] for d in data], [
             d["AvgCarbonIntensity"] for d in data])
    plt.xlabel("Time")
    plt.gca().xaxis.set_major_formatter(mdates.DateFormatter('%H:%M'))
    plt.ylabel("Average Carbon Intensity (kgCO2/MWh)")
    plt.title("CAISO Reported ACI for 10-Jul-2022")
    plt.savefig("CAISO_Reported_2022_07_10.png")
    # plt.show()


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "caiso_year":
        year_caiso_lowest_intensity()
    elif len(sys.argv) > 1 and sys.argv[1] == "caiso_day":
        day_caiso_intensity()
    else:
        main()
