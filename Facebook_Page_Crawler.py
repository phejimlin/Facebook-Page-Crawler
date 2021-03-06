import requests, os, time, json
import argparse, sys
from datetime import datetime
import elasticsearch

from multiprocessing import Pool
sys.setrecursionlimit(2000)
##########################################################################################################
def getRequests(url):
    try:
        requests_result = requests.get(url, headers={'Connection':'close'}).json()
    except ValueError:
        print("Requests value error.")
        return {}
    time.sleep(0.1)

    # Handle request limit reached
    error = requests_result.get('error')
    if error is not None:
        code = requests_result.get('error', {}).get('code')
        if code == 17:
            print(url)
            print(requests_result)
            print('Wait 10 min.')
            time.sleep(600)
            return getRequests(url)
        else:
            print(requests_result)
            return {}

    return requests_result

##########################################################################################################
def getFeedIds(feeds, feed_list):

    feeds = feeds['feed'] if 'feed' in feeds else feeds

    feed_dataset = feeds.get('data')
    if feed_dataset is None:
        print("There isn't feed return from FB api.")
        return []

    for feed in feed_dataset:
        feed_list.append(feed['id'])
        if not stream:
            print('Feed found: ' + feed['id'] + '\n')
            #log.write('Feed found: ' + feed['id'] + '\n')
    
    if 'paging' in feeds and 'next' in feeds['paging']:
        feeds_url = feeds['paging']['next']
        feed_list = getFeedIds(getRequests(feeds_url), feed_list)

    return feed_list

##########################################################################################################
def message_tags_worker(message, message_tags):
    # Replace user name with <user_name> and return clean message
    clean_message = ''
    start = 0
    for tag in message_tags:
        length = tag['length']
        offset = tag['offset']
        clean_message = clean_message + message[start:offset] + '<user_name>'
        start = start + length + offset
    clean_message = clean_message + message[offset + length:]
    return clean_message

##########################################################################################################
def getComments(dataset, comments_count, post_id):

    # If comments exist.
    comments = dataset['comments'] if 'comments' in dataset else dataset
    if 'data' in comments:
        if not stream and not es_flag:
            comments_dir = 'comments/'
            if not os.path.exists(comments_dir):
                os.makedirs(comments_dir)

        for comment in comments['data']:

            # Remove name in the message
            message_tags = comment.get("message_tags")
            if message_tags is not None:
                comment['message'] = message_tags_worker(comment['message'], message_tags)

            comment_content = {
                'id': comment['id'],
                'user_id': comment['from']['id'],
                'user_name': comment['from']['name'] if 'name' in comment['from'] else None,
                'message': comment['message'],
                'created_time': datetime.strftime(datetime.strptime(comment['created_time'], '%Y-%m-%dT%H:%M:%S+%f'), '%Y-%m-%d %H:%M:%S'),
                'inserted_time': datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
            }

            comments_count+= 1

            if stream:
                print(comment_content)
            elif es_flag:
                comment_content['post_id'] = post_id
                # get reply_comment
                # https://graph.facebook.com/v2.10/242305665805605_134537807167260/comments?access_token=
                reply_comment_url = "https://graph.facebook.com/v2.10/" + comment['id'] + "/comments?fields=created_time,from,message,id,message_tags&" + long_lived_token
                reply_comment_count = get_comments_comments(getRequests(reply_comment_url), 0, comment['id'])
                comment_content['reply_comment_count'] = reply_comment_count
                # get reaction of comment
                reply_reactions_count_dict = {
                    'like': 0,
                    'love': 0,
                    'haha': 0,
                    'wow': 0,
                    'sad': 0,
                    'angry': 0
                }
                reply_comment_reactions_url = "https://graph.facebook.com/v2.10/" + comment['id'] + '?fields=reactions.limit(100)&' + token
                # https://graph.facebook.com/v2.10/134537807167260_134647173822990/?fields=reactions&access_token=
                reply_reactions_count_dict = getReactions(getRequests(reply_comment_reactions_url), reply_reactions_count_dict, "source_id", comment['id'])
                comment_content.update(reply_reactions_count_dict)

                es.update(index=es_index, doc_type=es_comment_and_reaction_doc_type, id=comment_content['id'], body={'doc': comment_content, 'doc_as_upsert':True})
            else:
                print('Processing comment: ' + comment['id'] + '\n')
                comment_file = open(comments_dir + comment['id'] + '.json', 'w')
                comment_file.write(json.dumps(comment_content, indent = 4, ensure_ascii = False))
                comment_file.close()
                #log.write('Processing comment: ' + feed_id + '/' + comment['id'] + '\n')

        # Check comments has next or not.
        if 'next' in comments['paging']:
            comments_url = comments['paging']['next']
            comments_count = getComments(getRequests(comments_url), comments_count, post_id)

    return comments_count

##########################################################################################################
def getReactions(dataset, reactions_count_dict, source, id):

    # If reactions exist.
    reactions = dataset['reactions'] if 'reactions' in dataset else dataset
    if 'data' in reactions:
        if not stream and not es_flag:
            reactions_dir = 'reactions/'
            if not os.path.exists(reactions_dir):
                os.makedirs(reactions_dir)

        for reaction in reactions['data']:

            if reaction['type'] == 'LIKE':
                reactions_count_dict['like']+= 1
            elif reaction['type'] == 'LOVE':
                reactions_count_dict['love']+= 1
            elif reaction['type'] == 'HAHA':
                reactions_count_dict['haha']+= 1
            elif reaction['type'] == 'WOW':
                reactions_count_dict['wow']+= 1
            elif reaction['type'] == 'SAD':
                reactions_count_dict['sad']+= 1
            elif reaction['type'] == 'ANGRY':
                reactions_count_dict['angry']+= 1

            if stream:
                print(reaction)
            elif es_flag:
                reaction[source] = id
                
                # Change reation data format to be same as comment data format
                user_id = reaction.get("id", None)
                if (user_id is None):
                    print("Reactions id is None!!")
                    print(reaction)
                else:
                    reaction['user_id'] = user_id
                reaction.pop('id', None)

                user_name = reaction.get('name', None)
                if (user_name is None):
                    print("Reactions name is None!!")
                    print(reaction)
                else:
                    reaction['user_name'] = user_name
                reaction.pop('name', None)

                # query comment, if exist upsert with reaction
                res_query_comment = es.search(index=es_index, doc_type=es_comment_and_reaction_doc_type, body={"query": {
                    "bool": {
                        "must": [
                            {
                                "match": {
                                    source: id
                                }
                            },
                            {
                                "match": {
                                    "user_id": user_id
                                }
                            }
                        ]
                    }
                }
                })

                total = res_query_comment.get('hits', {}).get('total', None)
                if total is not None and total > 0:
                    search_result = res_query_comment['hits']['hits'][0]
                    comment_id = search_result['_id']
                    es.update(index=es_index, doc_type=es_comment_and_reaction_doc_type, id=comment_id, body={'doc':reaction, 'doc_as_upsert':True})
                # We don't store reaction to es. if you want, uncomment else.
                # else:
                #     es.index(index=es_index, doc_type=es_comment_and_reaction_doc_type, body=reaction)
            else:
                print('Processing reaction: ' + reaction['id'] + '\n')
                reaction_file = open(reactions_dir + reaction['id'] + '.json', 'w')
                reaction_file.write(json.dumps(reaction, indent = 4, ensure_ascii = False))
                reaction_file.close()
                #log.write('Processing reaction: ' + feed_id + '/' + reaction['id'] + '\n')

        # Check reactions has next or not.
        if 'next' in reactions['paging']:
            reactions_url = reactions['paging']['next']
            reactions_count_dict = getReactions(getRequests(reactions_url), reactions_count_dict, source, id)
            
    return reactions_count_dict

##########################################################################################################
def getAttachments(attachments, attachments_content):

    # If attachments exist.
    attachments = attachments['attachments'] if 'attachments' in attachments else attachments
    if 'data' in attachments:
        attachments_content['title'] = attachments['data'][0]['title'] if 'title' in attachments['data'][0] else ''
        attachments_content['description'] = attachments['data'][0]['description'] if 'description' in attachments['data'][0] else ''
        attachments_content['target'] = attachments['data'][0]['target']['url'] if 'target' in attachments['data'][0] and 'url' in attachments['data'][0]['target'] else ''

    return attachments_content


def get_comments_comments(dataset, comments_count, comment_id):
    # If comments exist.
    if 'data' in dataset and len(dataset['data'])>0:
        if not stream and not es_flag:
            comments_dir = 'comments/'
            if not os.path.exists(comments_dir):
                os.makedirs(comments_dir)

        for comment in dataset['data']:

            # Remove name in the message
            message_tags = comment.get("message_tags")
            if message_tags is not None:
                comment['message'] = message_tags_worker(comment['message'], message_tags)

            comment_content = {
                'id': comment['id'],
                'user_id': comment['from']['id'],
                'user_name': comment['from']['name'] if 'name' in comment['from'] else None,
                'message': comment['message'],
                'created_time': datetime.strftime(datetime.strptime(comment['created_time'], '%Y-%m-%dT%H:%M:%S+%f'), '%Y-%m-%d %H:%M:%S'),
                'inserted_time': datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
            }

            comments_count+= 1

            if stream:
                print(comment_content)
            elif es_flag:
                comment_content['source_id'] = comment_id
                es.update(index=es_index, doc_type=es_comment_and_reaction_doc_type, id=comment_content['id'], body={'doc': comment_content, 'doc_as_upsert':True})
            else:
                print('Processing comment: ' + comment['id'] + '\n')
                comment_file = open(comments_dir + comment['id'] + '.json', 'w')
                comment_file.write(json.dumps(comment_content, indent = 4, ensure_ascii = False))
                comment_file.close()
                #log.write('Processing comment: ' + feed_id + '/' + comment['id'] + '\n')

        # Check comments has next or not.
        if 'next' in dataset['paging']:
            comments_url = dataset['paging']['next']
            comments_count = get_comments_comments(getRequests(comments_url), comments_count, comment_id)

    return comments_count

##########################################################################################################
def getFeed(feed_id):

    feed_url = 'https://graph.facebook.com/v2.7/' + feed_id

    if not stream and not es_flag:
        feed_dir = feed_id + '/'
        if not os.path.exists(feed_dir):
            os.makedirs(feed_dir)

        os.chdir(feed_dir)

        print('\nProcessing feed: ' + feed_id + '\nAt: ' + datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S') + '\n')
        log = open('../log', 'a')
        log.write('\nProcessing feed: ' + feed_id + '\nAt: ' + datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S') + '\n')
        log.close()

    # query post_id check exist or not
    if es_flag:
        query_res = es.get(index=es_index, doc_type=es_post_doc_type, ignore=404, id=feed_id)
        if query_res.get('found') is True:
            # post exist so skip.
            print(str(feed_id) + " exist, so skip it.")
            return None
        else:
            print('crawling feed_id: ' + str(feed_id))


    # For comments.
    comments_url = feed_url + '?fields=comments.limit(100){from,created_time,message_tags,message,id}&' + long_lived_token
    # https://graph.facebook.com/v2.7/242305665805605_1235637133139115/?fields=comments.limit(100)&access_token=
    comments_count = getComments(getRequests(comments_url), 0, feed_id)

    # For reactions.
    if get_reactions:
        reactions_count_dict = {
            'like': 0,
            'love': 0,
            'haha': 0,
            'wow': 0,
            'sad': 0,
            'angry': 0
        }
        reactions_url = feed_url + '?fields=reactions.limit(100)&' + token
        # https://graph.facebook.com/v2.7/242305665805605_1235637133139115/?fields=reactions.limit(100)&access_token=
        reactions_count_dict = getReactions(getRequests(reactions_url), reactions_count_dict, "post_id", feed_id)
    
    # For attachments.
    attachments_content = {
        'title': '',
        'description': '',
        'target': ''
    }
    attachments_url = feed_url + '?fields=attachments&' + token
    attachments_content = getAttachments(getRequests(attachments_url), attachments_content)

    # For feed content.
    feed = getRequests(feed_url + '?' + token)

    if 'message' in feed:
        feed_content = {
            'id': feed['id'],
            'message': feed['message'],
            'link': feed['link'] if 'link' in feed else None,
            'created_time': datetime.strftime(datetime.strptime(feed['created_time'], '%Y-%m-%dT%H:%M:%S+%f'), '%Y-%m-%d %H:%M:%S'),
            'comments_count': comments_count,
            'inserted_time': datetime.strftime(datetime.now(), '%Y-%m-%d %H:%M:%S')
        }

        feed_content.update(attachments_content)

        if get_reactions:
            feed_content.update(reactions_count_dict)

        if stream:
            print(feed_content)
        elif es_flag:
            # Handle 'ascii' codec exception.
            try:
                print(feed_content)
            except Exception as e:
                print(e)
                print(feed_content['id'])

            es.index(index=es_index, doc_type=es_post_doc_type, id=feed_content['id'], body=feed_content)
        else:
            feed_file = open(feed_id + '.json', 'w')
            feed_file.write(json.dumps(feed_content, indent = 4, ensure_ascii = False))
            feed_file.close()

    if not stream:
        os.chdir('../')

##########################################################################################################
def getTarget(target):

    if not stream:
        target_dir = target + '/'
        if not os.path.exists(target_dir):
            os.makedirs(target_dir)
        os.chdir(target_dir)

        log = open('log', 'w')
        start_time = datetime.now()
        execution_start_time = time.time()
        print('Task start at:' + datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S') + '\nTaget: ' + target + '\nSince: ' + since + '\nUntil: ' + until + '\n')
        log.write('Task start at:' + datetime.strftime(start_time, '%Y-%m-%d %H:%M:%S') + '\nTaget: ' + target + '\nSince: ' + since + '\nUntil: ' + until + '\n')
        log.close()

    #Get list of feed id from target.
    feeds_url = 'https://graph.facebook.com/v2.7/' + target + '/?fields=feed.limit(100).since(' + since + ').until(' + until + '){id}&' + token
    feed_list = getFeedIds(getRequests(feeds_url), [])

    if not stream:
        feed_list_file = open('feed_ids', 'w')
        for id in feed_list:
            feed_list_file.write(id + '\n')
        feed_list_file.close()

    #Get message, comments and reactions from feed.
    target_pool = Pool()
    target_pool.map(getFeed, feed_list)
    target_pool.close()

    if not stream:
        end_time = datetime.now()
        cost_time = time.time() - execution_start_time
        print('\nTask end Time: ' + datetime.strftime(end_time, '%Y-%m-%d %H:%M:%S') + '\nTime Cost: ' + str(cost_time))
        log = open('log', 'a')
        log.write('\nTask end Time: ' + datetime.strftime(end_time, '%Y-%m-%d %H:%M:%S') + '\nTime Cost: ' + str(cost_time))
        log.close()
        os.chdir('../')


##########################################################################################################
if __name__ == '__main__':
    # Set crawler target and parameters.
    parser = argparse.ArgumentParser()

    parser.add_argument("target", help="Set the target fans page(at least one) you want to crawling. Ex: 'appledaily.tw' or 'appledaily.tw, ETtoday'")
    parser.add_argument("since", help="Set the start date you want to crawling. Format: 'yyyy-mm-dd HH:MM:SS'")
    parser.add_argument("until", help="Set the end date you want to crawling. Format: 'yyyy-mm-dd HH:MM:SS'")

    parser.add_argument("-r", "--reactions", help="Collect reactions or not. Default is no.")
    parser.add_argument("-s", "--stream", help="If yes, this crawler will turn to streaming mode.")
    parser.add_argument("-e", "--elasticsearch", help="If yes, this crawler will store data to elasticsearch.")

    args = parser.parse_args()

    target = str(args.target)
    since = str(args.since)
    until = str(args.until)

    if args.reactions == 'yes':
        get_reactions = True
    else:
        get_reactions = False

    if args.stream == 'yes':
        stream = True
    else:
        stream = False

    if args.elasticsearch == 'yes':
        es_flag = True
        # Set Elasticsearch config
        es = elasticsearch.Elasticsearch(timeout=30, max_retries=10, retry_on_timeout=True)
        es_index = 'facebook'
        es_post_doc_type = 'post'
        es_comment_and_reaction_doc_type = 'action'
        # Load mapping
        with open("./template/facebook_template.json", mode='r') as mapping:
            index_data = json.load(mapping)
        es.indices.create(index=es_index, ignore=400, body=index_data)
    else:
        es_flag = False

    app_id = 'YOUR_APP_ID'
    app_secret = 'YOUR_APP_SECRET'
    user_long_lived_token = 'YOUR_LONG_LIVED_TOKEN'
    
    long_lived_token = 'access_token=' + user_long_lived_token
    token = 'access_token=' + app_id + '|' + app_secret


    # Create a directory to restore the result if not in stream mode.
    if not stream:
        result_dir = 'Result/'
        if not os.path.exists(result_dir):
            os.makedirs(result_dir)
        os.chdir(result_dir)

    if target.find(',') == -1:

        getTarget(target)
        
    else:

        target = target.split(',')
        for t in target :
            getTarget(t)
