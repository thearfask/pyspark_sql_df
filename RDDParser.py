
def parseLineCustomer(line):
    fields = line.split("|")
    customerID = fields[0]
    customerFirstName = fields[1]
    customerLastName = fields[2]
    phoneNumber = fields[3]
    return (int(customerID),customerFirstName,customerLastName, int(phoneNumber))

def getCustomerColumns():
    return ["CustomerID", "CustomerFirstName", "CustomerLastName", "PhoneNumber"]

def parseLineProduct(line):
    fields = line.split("|")
    productID = fields[0]
    productName = fields[1]
    productType = fields[2]
    productVersion = fields[3]
    productPrice = fields[4].split("$")[1]
    return (int(productID), productName, productType, productVersion, int(productPrice))

def getProductColumns():
    return ["ProductID", "ProductName", "ProductType", "ProductVersion", "ProductPrice"]

def parseLineSales(line):
    fields = line.split("|")
    transactionID = fields[0]
    customerID = fields[1]
    productID = fields[2]
    timestamp = fields[3]
    totalAmt_split = fields[4].split("$")
    if len(totalAmt_split) > 1:
        totalAmount = totalAmt_split[1]
    else:
        totalAmount = fields[4]
    totalQuantity = fields[5]
    return (int(transactionID), int(customerID), int(productID), str(timestamp), int(totalAmount), int(totalQuantity))

def getSalesColumns():
    return ["TransactionID", "CustomerID", "ProductID","Timestamp","TotalAmount","TotalQuantity"]

def parseLineRefund(line):
    fields = line.split("|")
    refundID = int(fields[0])
    orgTransactionID = int(fields[1])
    customerID = int(fields[2])
    productID = int(fields[3])
    refundTimestamp = fields[4]
    refundAmount_split = fields[5].split("$")
    if len(refundAmount_split) > 1:
        refundAmount = refundAmount_split[1]
    else:
        refundAmount = fields[5]
    refundQuantity = int(fields[6])
    return (refundID, orgTransactionID, customerID,productID,str(refundTimestamp),refundAmount, refundQuantity)

def getRefundColumns():
    return ["RefundID", "OrgTransactionID", "CustomerID","ProductID","RefundTimestamp","RefundAmount", "RefundQuantity"]

