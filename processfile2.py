import codecs
import json

import util_dynamodb

import queueutil


def processFileContent(fcontent, filename):
    filebody = fcontent['Body']
    lcount:int =1
    dynamo = util_dynamodb.getDynamoDB()
    
    fn_trnno = util_dynamodb.getTrnHeaderFieldName(dynamo, 'TRNNO')
    fn_trndate = util_dynamodb.getTrnHeaderFieldName(dynamo, 'TRNDATE')
    fn_location = util_dynamodb.getTrnHeaderFieldName(dynamo, 'LOCATION')
    fn_filename = util_dynamodb.getTrnHeaderFieldName(dynamo, 'FILENAME')
    fn_shopamount = util_dynamodb.getTrnHeaderFieldName(dynamo, 'SHOPAMOUNT')
    fn_shopdiscount = util_dynamodb.getTrnHeaderFieldName(dynamo, 'SHOPDISCOUNT')
    fn_itemcode = util_dynamodb.getTrnLinesFieldName(dynamo, 'ITEMCODE')
    fn_orderno = util_dynamodb.getTrnLinesFieldName(dynamo, 'ORDERNO')
    print ("Fields " + fn_trnno + fn_trndate + fn_location + fn_filename)
    trnmsg = '{"Transaction":'
    for ln in codecs.getreader('utf-8')(filebody):
        #print(ln)
        l_values = ln.split("#")
        print((l_values))
        if (lcount==1):
            l_amount = 0.00
            l_discount = 0.00
            if (len(l_values) > 3 and len(str(l_values[3]).strip()) > 0) :
                l_amount = l_values[3]
            if (len(l_values) > 4 and len(str(l_values[4]).strip()) > 0):
                l_discount = l_values[4]
            #l_trnno = insertTrnHeader(c, l_values[1], l_values[2], filename)
            #print("TRn no = " + str(l_trnno))
            trnmsg += '{"' + fn_trnno + '":' + str(0) + ', "' + fn_location + '" : ' + str(l_values[1]) + ', "' + fn_trndate + '" : "' + \
            l_values[2] + '", "' + fn_filename + '" : "' + filename + '"' + ', "' + fn_shopamount + '" : ' + str(l_amount) + \
                ', "' + fn_shopdiscount + '" : ' + str(l_discount)
            trnmsg += ' , "lines":['

        #print("Item " + l_values[0] + " , Order = " + l_values[1])
        if (lcount>1):
            if (lcount > 2):
                trnmsg += ","
            trnmsg += '{"' + fn_itemcode + '":' + str(l_values[0]) + ', "' + fn_orderno + '" :' + str(l_values[1]) + '}'
            #insertTrnLines(c, l_trnno, lcount, l_values[0])
        lcount = lcount + 1

    trnmsg += "]}}"
    print(trnmsg)
    js = json.loads(trnmsg)
    #print(js['Transaction'])
    trns = js['Transaction']
    #print(trns['trn'])
    lines = trns['lines']
    #print(lines[3]['itemcode'])
    queueurl = queueutil.get_queue_url()
    queueutil.sendMsg(queueurl, trnmsg)
    #queueutil.recvMsg(queueurl)
