DEFAULT_FORMAT = ' [%(module)14s:%(lineno)3d] %(levelname)8s: %(message)s'
def buildFormatString(app_name):
    app_name = '%-8s' % app_name.upper()
    return '%(asctime)s - ' + app_name + DEFAULT_FORMAT
