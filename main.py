# coding: utf-8

__author__ = 'Ning <ninghuan@me.com>'

import json
import requests

import tornado.ioloop
import tornado.web

CLIENT_ID = 'e34923def93217191e73'
CLIENT_SECRET = 'eb19a84a3f89b72239bd9d0c97eccfa7a887063b'

payload = {
    'client_id': CLIENT_ID,
    'client_secret': CLIENT_SECRET
    }


def requests_get(url, headers=None):
    payload = {
        'client_id': CLIENT_ID,
        'client_secret': CLIENT_SECRET
    }

    return requests.get(url, params=payload, headers=headers)


class MainHandler(tornado.web.RequestHandler):
    def get(self):
        r = requests_get('https://api.github.com/users')
        while True:
            r_json = json.loads(r.text)
            for i in range(len(r_json)):
                id = r_json[i]['id']
                login = r_json[i]['login']
                avatar_url = r_json[i]['avatar_url']
                print id
                # todo:mongodb

                # 拿用户
                user = requests_get('https://api.github.com/users/%s' % login)
                user_json = json.loads(user.text)
                followers_count = user_json['followers']
                following_count = user_json['following']
                created_at = user_json['created_at']
                updated_at = user_json['updated_at']
                # todo:mongodb

                # 拿用户的粉丝
                followers = requests_get(user_json['followers_url'],
                                         )
                followers_json = json.loads(followers.text)
                for i in range(len(followers_json)):
                    follower_login = followers_json[i]['login']
                    # todo:mongodb

                # 拿用户的关注者
                following = requests_get(user_json['following_url'],
                                         )
                following_json = json.loads(following.text)
                for i in range(len(following_json)):
                    following_login = following_json[i]['login']
                    # todo:mongodb

                # 拿与用户相关的库
                repos = requests_get(user_json['repos_url'],
                                     )
                repos_json = json.loads(repos.text)
                for i in range(len(repos_json)):
                    name = repos_json[i]['name']
                    contributors = requests_get(repos_json['contributors_url'],
                                                )
                    contributors_json = json.loads(contributors.text)
                    contributors_login = []
                    for i in range(len(contributors_json)):
                        contributors_login.append(contributors_json[i]['login'])
                    if login in contributors_login:
                        repos_created = name
                        # todo:mongodb
                    else:
                        repos_following = name
                        # todo:mongodb

            next_link = r.headers['link']
            if next_link:
                r = requests_get(
                    next_link.split(';')[0][1:-1]
                )
            else:
                break

application = tornado.web.Application([
    (r'/', MainHandler)
])

if __name__ == '__main__':
    application.listen(8888)
    tornado.ioloop.IOLoop.current().start()

