class NeoException(Exception): pass
class ElectionFailure(NeoException): pass
class PrimaryFailure(NeoException): pass
class VerificationFailure(NeoException): pass
