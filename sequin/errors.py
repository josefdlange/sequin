class EntityError(Exception):
    pass


class EntityAlreadyExistsError(EntityError):
    pass


class EntityStaleError(EntityError):
    pass

# End
