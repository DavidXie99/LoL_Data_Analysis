import signal


class GracefulKiller:
    kill_now = False

    def __init__(self):

        signal.signal(signal.SIGINT, self.exit_gracefully)
        signal.signal(signal.SIGTERM, self.exit_gracefully)

    def exit_gracefully(self,signum, frame):
        print("Setting 'kill_now' to true")
        self.kill_now = True