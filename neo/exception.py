class NeoException(Exception): pass
class ElectionFailure(NeoException): pass
class PrimaryFailure(NeoException): pass
class VerificationFailure(NeoException): pass
class OperationFailure(NeoException): pass
class DatabaseFailure(NeoException): pass
