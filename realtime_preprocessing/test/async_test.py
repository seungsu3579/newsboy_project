import asyncio
from time import time
from pororo import Pororo



async def create_summ(return_summ: list, thread_id : int):
    summ = Pororo(task="summarization", model="extractive", lang="ko")
    return_summ[thread_id] = summ

    return return_summ

async def create_ner(return_ner: list, thread_id : int):
    ner = Pororo(task="ner", lang="ko")
    return_ner[thread_id] = ner

    return return_ner

async def create_pos(return_pos: list, thread_id : int):
    pos = Pororo(task="pos", lang="ko")
    return_pos[thread_id] = pos

    return return_pos

async def create_se(return_se: list, thread_id : int):
    se = Pororo(task="sentence_embedding", lang="ko")
    return_se[thread_id] = se

    return_se

async def create_summ_keyword_pororo(thread_num : int):

    pororo_summ_list = [0 for th in range(thread_num)]
    pororo_ner_list = [0 for th in range(thread_num)]
    pororo_pos_list = [0 for th in range(thread_num)]
    pororo_se_list = [0 for th in range(thread_num)]

    futures = [asyncio.ensure_future(create_summ(pororo_summ_list, th)) for th in range(thread_num)] + [asyncio.ensure_future(create_ner(pororo_ner_list, th)) for th in range(thread_num)] + [asyncio.ensure_future(create_pos(pororo_pos_list, th)) for th in range(thread_num)] + [asyncio.ensure_future(create_se(pororo_se_list, th)) for th in range(thread_num)]
    
    result = await asyncio.gather(*futures)


    pororo_object = [dict() for th in range(thread_num)]

    for i, tmp_dict in enumerate(pororo_object):

        tmp_dict["summ"] = pororo_summ_list[i]
        tmp_dict["ner"] = pororo_ner_list[i]
        tmp_dict["pos"] = pororo_pos_list[i]
        tmp_dict["se"] = pororo_se_list[i]
    
    return pororo_object

def test(num: int):
    tmp = []
    for i in range(num):
        summ = Pororo(task="summarization", model="extractive", lang="ko")
        ner = Pororo(task="ner", lang="ko")
        pos = Pororo(task="pos", lang="ko")
        tmp += [summ, ner, pos]
    
    return tmp

if __name__ == "__main__":
    begin = time()
    loop = asyncio.get_event_loop()
    v = loop.run_until_complete(create_summ_keyword_pororo(4))
    loop.close()
    # test(2)
    end = time()
    print('실행 시간: {0:.3f}초'.format(end - begin))