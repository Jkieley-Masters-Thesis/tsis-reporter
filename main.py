import csv
import logging
import os

from pclusterutils import LogUtil

LogUtil.config_loggin("mcs.log")

import threading
from sflower import PolicyHandler, Clusters, ReadKubeConfigUtil

watch_interval = 5.0  # seconds


def main():
    logging.info('starting')
    cluster = Clusters.get_scale_from_cluster()
    jobs = cluster.get_jobs()
    logging.info("test")

    report_jobs = get_simple_job_data(jobs)
    report_jobs = list(filter(lambda x: x['succeeded'] == 1, report_jobs))
    report_jobs_by_image = group_jobs_by_image_name(report_jobs)

    report_roll_up_stats = {}
    for image, jobs in report_jobs_by_image.items():
        durationSeconds = list(map(lambda x: x['durationSeconds'], jobs))
        max_value = max(durationSeconds)
        min_value = min(durationSeconds)
        avg_value = 0 if len(durationSeconds) == 0 else sum(durationSeconds) / len(durationSeconds)

        report_roll_up_stats[image] = {
            'image': image,
            'count': len(jobs),
            'max': max_value,
            'min': min_value,
            'avg': avg_value,
        }

    report_list = list(report_roll_up_stats.values())

    rollup_csv_output = list_to_csv_output(report_list)
    ReadKubeConfigUtil.write_text_to_file(rollup_csv_output, 'rollup.csv')

    job_csv_output = list_to_csv_output(report_jobs)
    ReadKubeConfigUtil.write_text_to_file(job_csv_output, 'jobs.csv')


    logging.info("test")


def list_to_csv_output(report_list):
    csv_output = ""
    csv_output += ",".join(list(report_list[0].keys())) + "\n"
    for report_item in report_list:
        csv_output += ",".join(list(map(lambda x: str(x), report_item.values()))) + "\n"
    return csv_output


def group_jobs_by_image_name(report_jobs):
    report_jobs_by_job = {}
    for job in report_jobs:
        if job['image'] not in report_jobs_by_job:
            report_jobs_by_job[job['image']] = []
        report_jobs_by_job[job['image']].append(job)
    return report_jobs_by_job


def get_simple_job_data(jobs):
    report_jobs = []
    for job in jobs:
        job_info = {
            'image': job.spec.template.spec.containers[0].image,
            'name': job.metadata.name,
            'start_time': job.status.start_time,
            'succeeded': job.status.succeeded,
            'completion_time': job.status.completion_time
        }
        job_info['durationSeconds'] = (job_info['completion_time'] - job_info['start_time']).total_seconds()

        report_jobs.append(job_info)
    return report_jobs


if __name__ == '__main__': main()
