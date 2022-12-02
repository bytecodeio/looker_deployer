def find_target_dash(source_dash_id, source_sdk, target_sdk):
    source_dash = source_sdk.dashboard(source_dash_id)
    target_dash = target_sdk.search_dashboards(slug=source_dash['slug'])
    if len(target_dash) == 0:
        target_dash = target_sdk.search_dashboards(title=source_dash['title'])
    if len(target_dash) > 1:
        target_folder = find_target_folder(source_dash['folder']['name'], source_dash['folder']['parent_id'], source_sdk, target_sdk)
        if len(target_folder) == 0:
            return target_dash
        #elif len(target_folder) > 1:
        else:
            target_dash = []
            for folder in target_folder:
                #print('##### target_dash len + folder_id ' + str(len(target_dash)) + ' ' + str(folder['id']))
                target_dash = target_dash + target_sdk.search_dashboards(title=source_dash['title'], folder_id=folder['id'])
    return target_dash


def find_target_look(source_look_id, source_sdk, target_sdk):
    source_look = source_sdk.look(source_look_id)
    target_look = target_sdk.search_looks(title=source_look['title'])
    if len(target_look) > 1:
        target_folder = find_target_folder(source_look['folder']['name'], source_look['folder']['parent_id'], source_sdk, target_sdk)
        if len(target_folder) == 0:
            return target_look
        #elif len(target_folder) > 1:
        else:
            target_look = []
            for folder in target_folder:
                #print('##### target_dash len + folder_id ' + str(len(target_dash)) + ' ' + str(folder['id']))
                target_look = target_look + target_sdk.search_looks(title=source_look['title'], folder_id=folder['id'])
    return target_look

def find_target_folder(source_folder_name, source_parent_id, source_sdk, target_sdk):
    #source_folder = source_sdk.folder(source_folder_id)
    target_folder = target_sdk.search_folders(name=source_folder_name)
    if len(target_folder) == 0:
        return target_folder
    if len(target_folder) > 1:
        target_folder = []
        source_parent = source_sdk.folder(source_parent_id)
        target_parent = target_sdk.search_folders(name=source_parent['name'])
        for parent in target_parent:
            print('######### target parent folder name ###########')
            print(parent['name'])
            target_folder = target_folder + target_sdk.search_folders(name=source_folder_name, parent_id=parent['id'])
    return target_folder
