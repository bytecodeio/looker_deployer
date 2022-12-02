import logging
import json
from looker_deployer.utils import deploy_logging
from looker_deployer.utils.get_client import get_client
from looker_deployer.utils.find_target_object import find_target_dash

logger = deploy_logging.get_logger(__name__)

def send_alerts(source_sdk, target_sdk, copy_disabled):
    if copy_disabled:
        source_alerts = source_sdk.search_alerts(all_owners=True)
    else:
        source_alerts = source_sdk.search_alerts(disabled=False, all_owners=True)

    target_alerts = target_sdk.search_alerts(all_owners=True)

    for alert in source_alerts:
        source_user = source_sdk.user(alert['owner_id'])
        if source_user['email'] != 'lookerbackorder@solostove.com':
            continue
        target_user = target_sdk.search_users(email=source_user['email'])
        if len(target_user) == 0:
            logger.info("Target user missing for alert", extra={"missing email": source_user['email'], "missing user_id": alert['owner_id'], "alert id": alert['id']})
            continue
        source_element = source_sdk.dashboard_element(alert['dashboard_element_id'])
        source_dash = source_sdk.dashboard(source_element['dashboard_id'])
        target_dash = find_target_dash(source_dash['id'], source_sdk, target_sdk)


        if len(target_dash) == 0:
            logger.info("Target dash not found", extra={"source dash title": source_dash['title'], "source dash folder": source_dash['folder']['name']})
            continue
        elif len(target_dash) > 1:
            logger.info("Ambigious target dashboard", extra={"source dash title": source_dash['title'], "source dash folder": source_dash['folder']['name']})
            continue
        else:
            target_dash_element = list(filter(lambda item: item['title'] == source_element['title'], target_dash[0]['dashboard_elements']))
            if len(target_dash_element) == 0:
                logger.info("Unable to locate target dashboard element", extra={"source dash title": source_dash['title'], "source dash element title": source_element['title']})
                continue
            elif len(target_dash_element) > 1:
                logger.info("Ambigious target dashboard element", extra={"source dash title": source_dash['title'], "source dash folder": source_dash['folder']['name'], "source dash element title": source_element['title']})
                continue
            else:
                new_alert = {}
                for key in alert.keys():
                    new_alert[key] = alert[key]
                new_alert['owner_id'] = target_user[0]['id']
                new_alert['dashboard_element_id'] = target_dash_element[0]['id']
                print (json.dumps(new_alert, indent=4, default=str))
                
                existing_element_alerts = list(filter(lambda alert: new_alert['dashboard_element_id'] == alert['dashboard_element_id'], target_alerts))
                print("length of existing alerts" + str(len(existing_element_alerts)))
                #if len(existing_element_alerts) == 0:
                #    target_sdk.create_alert(new_alert)
                #else:
                #    for alert in existing_element_alerts:
                if len(existing_element_alerts) > 0:
                    is_duplicate = False
                    break_out_flag = False
                    for alert in existing_element_alerts:
                        print("new crontab = " + str(new_alert['cron']) + " old crontab = " + str(alert['cron']))
                        print("new dest length = " + str(len(new_alert['destinations'])) + " existing dest length = " + str(len(alert['destinations'])))
                        if (new_alert['cron'] == alert['cron']) and (len(new_alert['destinations']) == len(alert['destinations'])):
                            #new_dests = []
                            for dest in new_alert['destinations']:
                                if dest not in alert['destinations']:
                                    break
                                else:
                                    break_out_flag = True
                            if break_out_flag:
                                is_duplicate = True
                                break

                    if is_duplicate:
                        continue
                    else:
                        print("made it to line 66")
                        print(new_alert)
                        target_sdk.create_alert(new_alert)
                else:
                    print("made it to line 69")
                    target_sdk.create_alert(new_alert)



    return



def main(args):

    if args.debug:
        logger.setLevel(logging.DEBUG)

    source_sdk = get_client(args.ini, args.source)

    for t in args.target:
        target_sdk = get_client(args.ini, t)

        send_alerts(source_sdk, target_sdk, args.copy_disabled)
