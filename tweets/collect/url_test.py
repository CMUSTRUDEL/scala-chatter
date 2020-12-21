if __name__ == '__main__':
    import db
    import requests
    from termcolor import colored as cr

    connection = db.mk_connection()
    urls = [t[0] for t in db.execute(connection, 'select url from tweets_url')]
    print(len(urls), 'URLs')
    connection.close()

    redirect_counter = 0
    seen_counter = 0
    for url in urls:
        seen_counter += 1
        try:
            r = requests.get(url, timeout = 5)
            if r.url != url:
                redirect_counter += 1
                print(
                        cr(redirect_counter, 'red', attrs = ['bold']),
                        '/',
                        cr(seen_counter, 'green', attrs = ['bold']),
                        url, '->', r.url,
                        flush = True
                )
        except requests.exceptions.Timeout:
            print('Timeout', url)
        except:
            print('Fail', url)

