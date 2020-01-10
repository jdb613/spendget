# import pandas as pd
import os
import pandas as pd
from os import environ
from dotenv import load_dotenv


from sendgrid.helpers.mail import Mail
from sendgrid import SendGridAPIClient
import python_http_client

import helpers

load_dotenv()

# keep_list = environ.get('KEEP_LIST').split(',')
# keep_list = [x.strip(' ') for x in keep_list]

new_transactions = helpers.frame_prep(helpers.getData(), environ.get('KEEP_LIST'))
helpers.addTransaction(new_transactions)

nt_styled = new_transactions.style.set_caption('New Transactions').set_table_styles(helpers.tableStyles()).set_table_attributes('border="1" class="dataframe table table-hover table-bordered"')
html_data = helpers.jinjaTEST(nt_styled.render())

if environ.get('RUN_MODE') == 'Production':
    message = Mail(
            from_email=os.getenv('SENDGRID_MAIL'),
            to_emails=os.getenv('SENDGRID_MAIL'),
            subject='New Transaction Report',
            html_content=html_data)

    try:
      sg = SendGridAPIClient(os.environ.get('SENDGRID_API_KEY'))
      response = sg.send(message)
      print('>>> Send Grid Response Data')
      print(response.status_code)
      print(response.body)
      print(response.headers)
      result = "Email Sent"
    except Exception as e:
      print('>>> SendGrid ERROR')
      result = str(e)

else:
  prev = open('templates/email_preview.html','w')
  prev.write(html_data)
  prev.close()
  result = 'Preview File Updated'

  print(result)
