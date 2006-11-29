from neo.handler import EventHandler

class MasterEventHandler(EventHandler):
    """This class implements a generic part of the event handlers."""
    def __init__(self, app):
        self.app = app
        EventHandler.__init__(self)
