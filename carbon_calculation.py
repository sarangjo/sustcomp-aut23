# Imports
import csv
import datetime
import random
import math
import matplotlib.pyplot as plt


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
        },
        "amd_epyc_7571": {
            "cores": 32,
            "embodied_carbon": 1610.40,
            "tdp": 120,  # Watts
            "tdp_coefficient": 0.3,
        },
    }

    AVG_HARDWARE_LIFE_YRS = random.randint(2, 7)
    AVG_HARDWARE_USAGE_PER_YR_HRS = datetime.timedelta(hours=8760)
    AVG_HARDWARE_LIFE = (AVG_HARDWARE_LIFE_YRS *
                         AVG_HARDWARE_USAGE_PER_YR_HRS)

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
            job["time"] / CloudScheduler.AVG_HARDWARE_LIFE) * hw["embodied_carbon"]

        return embodied

    def energy_consumed(job):
        # Returns MWh
        hw = CloudScheduler.hardware_specs[job["hardware"]]

        en = job["server_utilization"] * (job["time"] / datetime.timedelta(hours=1)) * \
            hw["cores"] * hw["tdp"] * hw["tdp_coefficient"] / 1e6
        return en

    def submit_job(self, job) -> bool:
        """
        Okay! Now let's just simply allocate with no brains. We need to specify some sort of capacity to
        limit how many jobs can all be allocated to the lowest emissions.

        Returns allocation and total carbon
        """

        # Step 1: get time
        time_required = job["time"]

        # This tells us how many slices we need. Let's keep it simple and just allocate to specific
        # slices with a certain proportion of its energy usage
        time_required_hrs = time_required / datetime.timedelta(hours=1)
        num_slots = math.ceil(time_required_hrs)
        full_slots = math.floor(time_required_hrs)

        energy_consumed = CloudScheduler.energy_consumed(job)
        # print("energy_consumed", energy_consumed)
        energy_per_hr = energy_consumed / time_required_hrs

        # Start with embodied carbon
        carbon = CloudScheduler.embodied_carbon(job)

        # Step 2: get best slots
        slots = self.get_best_slots_v1(method="dampened")[:num_slots]

        # Precompute the total carbon this "best allocation" would take
        for (idx, intensity) in slots[:full_slots]:
            carbon += energy_per_hr
        (idx, intensity) = slots[full_slots]
        carbon += intensity * energy_per_hr * (time_required_hrs % 1)

        # Is this possible?
        if carbon > job["carbon_budget"]:
            return False

        # Allocate job
        # Whole hours
        for (idx, intensity) in slots[:full_slots]:
            self.current_allocation[idx].energy += energy_per_hr
            self.current_allocation[idx].jobs.append(
                {"id": job["id"], "time": 1})

        # Leftovers
        (idx, intensity) = slots[full_slots]
        self.current_allocation[idx].energy += energy_per_hr * \
            (time_required_hrs % 1)
        self.current_allocation[idx].jobs.append(
            {"id": job["id"], "time": time_required_hrs % 1})

        print(self.current_allocation)
        print("carbon!", carbon)

        self.job_carbon.append(carbon)

        return carbon

    def get_best_slots_v1(self, method="plain"):
        """
        Simply by intensity (which is sorted) and curbing by capacity
        """
        # print("before:", [o["AvgCarbonIntensity"] for o in self.intensity_data])
        after = [(idx, self.get_adjusted_intensity(idx) if method ==
                  "dampened" else self.get_intensity(idx)) for idx in range(24)]
        # print("after: ", [o[1] for o in after])
        return sorted(after, key=lambda o: o[1])

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

    jobs = JOBS

    for j in jobs:
        print(j)
        scheduler.submit_job(j)

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
    plt.ylabel("Average Carbon Intensity (kgCO2/MWh)")
    plt.title("CAISO Reported ACI for 10-Jul-2022")
    plt.savefig("CAISO_Reported_2022_07_10.png")


if __name__ == "__main__":
    main()
