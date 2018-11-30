from elasticsearch import Elasticsearch
from datetime import datetime
import json

def create_index():
    """
    添加index
    """
    result = es.indices.create(index='news', ignore=400)  # 忽略这个错误，不抛出异常
    print(result)


def delete_index():
    """
    删除
    """
    result = es.indices.delete(index='news', ignore=[400, 404])
    print(result)
    # {'acknowledged': True}
    """{"erro": {"root_cause": [{"type": "index_not_found_exception", "reason": "no such index", "resource.type": "index_or_alias", "resource.id": "news", "index_uuid": "_na_", "index": "news"}], "type": "index_not_found_exception", "reason": "no such index","resource.type": "index_or_alias", "resource.id": "news", "index_uuid": "_na_","index": "news"}, "status": 404}
    """


def insert_data():
    """
    插入数据
    """
    es.indices.create(index='news', ignore=400)
    body = {
        'title': '这是一条新闻标题',
        'url': 'http://news.baidu.com/',
    }
    result = es.create(index='news', doc_type='politics', id=1, body=body)
    print(result)


def insert_data_index():
    """
    使用index来insert data
    """
    body = {
        'title': '这是一条google新闻标题',
        'url': 'www.google.com/',
    }
    result = es.index(index='news', doc_type='politics', body=body)  # 会自动生成一个id
    print(result)


def update_data():
    """
    更新数据
    """
    time_now = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    print(time_now)
    body = {
        'title': '这是一条新闻标题',
        'url': 'http://news.baidu.com/',
        'date': time_now,
    }
    result = es.index(index='news', doc_type='politics', body=body, id=1)
    print(result)


def delete_data():
    """
    删除数据
    :return:
    """
    result = es.delete(index='news', doc_type='politics', id=1)
    print(result)


def search_data():
    """
    查询数据
    :return:
    """
    mapping = {
        'properties': {
            'title': {
                'type': 'text',
                'analyzer': 'ik_max_word',
                'search_analyzer': 'ik_max_word'
            }
        }
    }
    # 配置analysis-ik
    es.indices.delete(index='news', ignore=[400, 404])
    es.indices.create(index='news', ignore=400)
    result = es.indices.put_mapping(index='news', doc_type='politics', body=mapping)
    print(result)


def insert_data02():
    datas = [
        {
            'title': '美国留给伊拉克的是个烂摊子吗',
            'url': 'http://view.news.qq.com/zt2011/usa_iraq/index.htm',
            'date': '2011-12-16'
        },
        {
            'title': '公安部：各地校车将享最高路权',
            'url': 'http://www.chinanews.com/gn/2011/12-16/3536077.shtml',
            'date': '2011-12-16'
        },
        {
            'title': '中韩渔警冲突调查：韩警平均每天扣1艘中国渔船',
            'url': 'https://news.qq.com/a/20111216/001044.htm',
            'date': '2011-12-17'
        },
        {
            'title': '中国驻洛杉矶领事馆遭亚裔男子枪击 嫌犯已自首',
            'url': 'http://news.ifeng.com/world/detail_2011_12/16/11372558_0.shtml',
            'date': '2011-12-18'
        }
    ]

    for data in datas:
        es.index(index='news', doc_type='politics', body=data)


def full_text_search():
    """
    全文检索
    :return:
    """
    dsl = {
        'query': {
            'match': {
                'title': '中国 领事馆'
            }
        }
    }
    result = es.search(index='news', doc_type='politics', body=dsl)
    print(json.dumps(result, indent=2, ensure_ascii=False))


if __name__ == '__main__':
    es = Elasticsearch()
    # create_index()
    # delete_index()
    # insert_data()
    # insert_data_index()
    # update_data()
    # delete_data()
    # search_data()
    # insert_data02()
    # result = es.search(index='news', doc_type='politics')
    # print(result)
    # print()
    # re = json.dumps(result)
    # print(re)
    full_text_search()
