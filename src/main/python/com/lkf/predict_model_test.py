from predict_model import Predict

json_str = {
  "predictFilter": [
    {
      "fieldName": "city",
      "fieldValue": [
        "天津"
      ]
    },
    {
      "fieldName": "store_code",
      "fieldValue": [
        "3621",
        "5379"
      ]
    },
    {
      "fieldName": "group_code",
      "fieldValue": [
        "7890"
      ]
    }
  ],
  "historyDate": {
    "fieldName": "settle_biz_date",
    "fieldValue": [
      "1561910400000",
      "1563206400000"
    ]
  },
  "predictDate": {
    "fieldName": "settle_biz_date",
    "fieldValue": [
      "1561910400000",
      "1563206400000"
    ]
  },
  "predictIndex": [
    {
      "fieldName": "amount"
    },
    {
      "fieldName": "weather"
    }
  ],
  "predictDimension": {
    "fieldName": "settle_biz_date"
  }
}

obj = Predict(json_str)
method = obj.turnover()
print(method)

