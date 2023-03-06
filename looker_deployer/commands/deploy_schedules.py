import logging
from looker_deployer.utils import deploy_logging
from looker_deployer.utils.get_client import get_client
from looker_deployer.utils.find_target_object import find_target_dash, find_target_look

logger = deploy_logging.get_logger(__name__)

def send_schedules(source_sdk, target_sdk, folders=None):
    logger.info("Lets copy some schedules!")

    source_schedules = source_sdk.all_scheduled_plans(all_users="true")
    logger.info("Number of schedules", extra={"schedule_count": len(source_schedules)})
    for schedule in source_schedules:
        source_user = source_sdk.user(schedule['user_id'])
        target_user = target_sdk.search_users(email=source_user['email'])
        if len(target_user) == 0:
            logger.info("Target user missing for schedule", extra={"missing email": source_user['email'], "missing user_id": schedule['user_id']})
            continue
        if schedule['look_id'] is not None:
            source_look = source_sdk.look(schedule['look_id'])
            target_look = find_target_look(schedule['look_id'], source_sdk, target_sdk)
            if len(target_look) == 0:
                logger.info("Target look not found", extra={"source look title": source_look['title'], "source look folder": source_look['folder']['name']})
                continue
            elif len(target_look) > 1:
                logger.info("Ambigious target look", extra={"source look title": source_look['title'], "source look folder": source_look['folder']['name']})
                continue
            else:
                new_schedule = {}
                logger.info("Time for a new schedule!")
                for key in schedule.keys():
                    new_schedule[key] = schedule[key]
                new_schedule['user_id'] = target_user[0]['id']
                new_schedule['look_id'] = target_look[0]['id']
                existing_schedules = target_sdk.scheduled_plans_for_look(new_schedule['look_id'], user_id=new_schedule['user_id'])
                if len(existing_schedules) > 0:
                    new_dests = []
                    for destination in new_schedule['scheduled_plan_destination']:
                        dest = {'format': destination['format'], 'address': destination['address']}
                        new_dests.append(dest)
                    is_duplicate = False
                    break_out_flag = False
                    for schedule in existing_schedules:
                        if (new_schedule['crontab'] == schedule['crontab']) and (len(new_schedule['scheduled_plan_destination']) == len(schedule['scheduled_plan_destination'])):
                            #new_dests = []
                            existing_dests = []
                            for destination in schedule['scheduled_plan_destination']:
                                dest = {'format': destination['format'], 'address': destination['address']}
                                existing_dests.append(dest)
                            for dest in new_dests:
                                if dest not in existing_dests:
                                    break
                                else:
                                    break_out_flag = True

                            if break_out_flag:
                                is_duplicate = True
                                break

                    if is_duplicate:
                        continue
                    else:
                        try:
                            target_sdk.create_scheduled_plan(new_schedule)
                        except Exception as e:
                            print(e)
                else:
                    try:
                        target_sdk.create_scheduled_plan(new_schedule)
                    except Exception as e:
                        print(e)
        if schedule['dashboard_id'] is not None:
            source_dash = source_sdk.dashboard(schedule['dashboard_id'])
            target_dash = find_target_dash(schedule['dashboard_id'], source_sdk, target_sdk)
            if len(target_dash) == 0:
                logger.info("Target dash not found", extra={"source dash title": source_dash['title'], "source dash folder": source_dash['folder']['name']})
                continue
            elif len(target_dash) > 1:
                logger.info("Ambigious target dashboard", extra={"source dash title": source_dash['title'], "source dash folder": source_dash['folder']['name']})
                continue
            else:
                new_schedule = {}
                logger.info("Time for a new schedule!")
                for key in schedule.keys():
                    new_schedule[key] = schedule[key]
                new_schedule['user_id'] = target_user[0]['id']
                new_schedule['dashboard_id'] = target_dash[0]['id']
                existing_schedules = target_sdk.scheduled_plans_for_dashboard(new_schedule['dashboard_id'], user_id=new_schedule['user_id'])
                if len(existing_schedules) > 0:
                    new_dests = []
                    for destination in new_schedule['scheduled_plan_destination']:
                        dest = {'format': destination['format'], 'address': destination['address']}
                        new_dests.append(dest)
                    is_duplicate = False
                    break_out_flag = False
                    for schedule in existing_schedules:
                        if (new_schedule['crontab'] == schedule['crontab']) and (len(new_schedule['scheduled_plan_destination']) == len(schedule['scheduled_plan_destination'])):
                            #new_dests = []
                            existing_dests = []
                            for destination in schedule['scheduled_plan_destination']:
                                dest = {'format': destination['format'], 'address': destination['address']}
                                existing_dests.append(dest)
                            for dest in new_dests:
                                if dest not in existing_dests:
                                    break
                                else:
                                    break_out_flag = True

                            if break_out_flag:
                                is_duplicate = True
                                break

                    if is_duplicate:
                        continue
                    else:
                        try:
                            target_sdk.create_scheduled_plan(new_schedule)
                        except Exception as e:
                            print(e)
                else:
                    try:
                        target_sdk.create_scheduled_plan(new_schedule)
                    except Exception as e:
                        print(e)


def main(args):

    if args.debug:
        logger.setLevel(logging.DEBUG)

    source_sdk = get_client(args.ini, args.source)

    for t in args.target:
        target_sdk = get_client(args.ini, t)

        send_schedules(source_sdk, target_sdk, args.folders)
