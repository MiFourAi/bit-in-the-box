import socket
import sys
import json
from BaseHTTPServer import BaseHTTPRequestHandler, HTTPServer

sys.path.append('../../tcaf/tcaf')
import loader
import utils


def main():
  loader.set_path('../../../captured_md')
  print loader.get_path()

  # set up server
  host, port = 'localhost', 5011

  start_datetime = '20180113T190000'
  end_datetime = '20180113T200000'
  product_id = 'btcusd'
  exchange = 'gdax'

  subscription = loader.Subscription(start_datetime, end_datetime)
  subscription.add_subscriber('OrderBook', [exchange], [product_id])
  subscription.add_subscriber('Trade', [exchange], [product_id])
  data_stream = subscription.process()

  class Handler(BaseHTTPRequestHandler):

    def _set_headers(self):
      self.send_response(200)
      self.send_header('Content-type', 'text')
      self.send_header('Access-Control-Allow-Origin', '*')
      self.send_header(
          'Access-Control-Allow-Methods',
          'GET, POST, PATCH, PUT, DELETE, OPTIONS')
      self.send_header(
          'Access-Control-Allow-Headers',
          'Origin, Content-Type, X-Auth-Token')
      self.end_headers()

    def do_GET(self):
      self._set_headers()
      try:
        md = next(data_stream)[1]
        d = {f: getattr(md, f) for f in md._fields}
        d['type'] = type(md).__name__
        d = json.dumps(d)
        self.wfile.write(
            '{}'.format(d))
      except StopIteration:
        self.wfile.write('')

    def do_OPTIONS(self):
      self._set_headers()

  server_address = (host, port)
  httpd = HTTPServer(server_address, Handler)
  print 'Starting httpd...'

  while True:
    try:
      httpd.handle_request()
    except KeyboardInterrupt:
      break


if __name__ == '__main__':
  main()
