import contextlib
from dataclasses import dataclass, field
import datetime
import os
from typing import Any, Iterable, Optional, List
from urllib.parse import urlparse
import re
import json

from ibridges.cli.config import IbridgesConf
from ibridges import IrodsPath, download, upload, Session
from ibridges.exception import DoesNotExistError, PasswordError
from ibridges.interactive import DEFAULT_IRODSA_PATH

from snakemake_interface_storage_plugins.settings import StorageProviderSettingsBase
from snakemake_interface_storage_plugins.storage_provider import (  # noqa: F401
    StorageProviderBase,
    StorageQueryValidationResult,
    ExampleQuery,
    Operation,
    QueryType,
)
from snakemake_interface_storage_plugins.storage_object import (
    StorageObjectRead,
    StorageObjectWrite,
    StorageObjectGlob,
    retry_decorator,
)
from snakemake_interface_storage_plugins.io import IOCacheStorageInterface


# env_msg = "Will also be read from ~/.irods/irods_environment.json if present."
METADATA_EXT=".metadata.json"

class ValueErrorParser():
    """For raising value errors instead of ArgumentParser errors.
    
    IbridgesConf is a class normally used for the command line interface (CLI).
    This ensures that errors normally passed to the argument parser, will be
    
    """
    def error(self, msg: str):
        raise ValueError(msg)

# Copied from python-irodsclient: client_init.py f700ab5
@contextlib.contextmanager
def _open_irodsa(file_path, *arg, **kw):
    """Open a file with 0o600 file permissions generally."""
    f = old_mask = None
    try:
        old_mask = os.umask(0o77)
        f = open(file_path, *arg, **kw)
        yield f
    finally:
        if old_mask is not None:
            os.umask(old_mask)
        if f is not None:
            f.close()


def _non_interactive_auth(settings):
    # iBridges doesn't have a non-interactive auth, so make one.
    ibridges_conf = IbridgesConf(ValueErrorParser())
    ienv_path, ienv_entry = ibridges_conf.get_entry()
    irodsa_backup = ienv_entry.get("irodsa_backup", None)
    cwd = ienv_entry.get("cwd", None)
    cwd = cwd if settings.cwd is None else settings.cwd
    if irodsa_backup is not None:
        with _open_irodsa(DEFAULT_IRODSA_PATH, "w", encoding="utf-8") as handle:
            handle.write(irodsa_backup)

    return Session(ienv_path, settings.password, settings.home, cwd)


def _switch_password(environment_file):
    """Write the password file .irodsA if a cached password is available to the CLI."""
    ibridges_conf = IbridgesConf(ValueErrorParser())
    _, ienv_entry = ibridges_conf.get_entry(environment_file)
    irodsa_backup = ienv_entry.get("irodsa_backup", None)
    if irodsa_backup is not None:
        with _open_irodsa(DEFAULT_IRODSA_PATH, "w", encoding="utf-8") as handle:
            handle.write(irodsa_backup)
        

@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    password: Optional[str] = field(
        default=None,
        metadata={
            "help": "The password for the iRODS server.",
            "env_var": True,
            "required": False,
        },
    )
    environment_file: Optional[str] = field(
        default=None,
        metadata={
            "help": "iRODS environment file to use for authentication",
            "env_var": True,
            "required": False,
        }
    )

    home: Optional[str] = field(
        default=None,
        metadata={
            "help": "The home parameter for the iRODS server.",
            "env_var": False,
            "required": False,
        },
    )
    cwd: Optional[str] = field(
        default=None,
        metadata={
            "help": "The current working directory on the iRODS server.",
            "env_var": True,
            "required": False,
        }
    )

# Required:
# Implementation of your storage provider
# This class can be empty as the one below.
# You can however use it to store global information or maintain e.g. a connection
# pool.
class StorageProvider(StorageProviderBase):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        try:
            if self.settings.environment_file is not None:
                if self.settings.password is None:
                    # Attempt to get password from CLI config file.
                    _switch_password(self.settings.environment_file)
                self.session = Session(self.settings.environment_file, self.settings.password, self.settings.home,
                                    self.settings.cwd)
            else:
                self.session = _non_interactive_auth(self.settings)
        except PasswordError as exc:
            raise PasswordError(
                "Failed to authenticate. Set or supply the password directly or"
                " authenticate once using the current/currently used irods environment file,"
                " which would cache the password depending on your authentication method."
            ) from exc

    @classmethod
    def example_queries(cls) -> List[ExampleQuery]:
        """Return an example query with description for this storage provider."""
        return [
            ExampleQuery(
                query="irods://folder/myfile.txt",
                type=QueryType.ANY,
                description="A file in a folder on the iRODS server.",
            ),
        ]

    def rate_limiter_key(self, query: str, operation: Operation) -> Any:
        """Return a key for identifying a rate limiter given a query and an operation.

        This is used to identify a rate limiter for the query.
        E.g. for a storage provider like http that would be the host name.
        For s3 it might be just the endpoint URL.
        """
        return self.session.host

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second for this storage
        provider."""
        return 100.0

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        return True

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Return whether the given query is valid for this storage provider."""
        # Ensure that also queries containing wildcards (e.g. {sample}) are accepted
        # and considered valid. The wildcards will be resolved before the storage
        # object is actually used.
        parsed = urlparse(query)
        if parsed.scheme == "irods" and parsed.path and parsed.netloc:
            return StorageQueryValidationResult(valid=True, query=query)
        else:
            return StorageQueryValidationResult(
                valid=False,
                query=query,
                reason="Query does not start with irods://, starts with irods:/// or does not "
                "contain a path to a file or directory.",
            )


# Required:
# Implementation of storage object. If certain methods cannot be supported by your
# storage (e.g. because it is read-only see
# snakemake-storage-http for comparison), remove the corresponding base classes
# from the list of inherited items.
class StorageObject(StorageObjectRead, StorageObjectWrite, StorageObjectGlob):
    # For compatibility with future changes, you should not overwrite the __init__
    # method. Instead, use __post_init__ to set additional attributes and initialize
    # futher stuff.

    def __post_init__(self):
        # This is optional and can be removed if not needed.
        # Alternatively, you can e.g. prepare a connection to your storage backend here.
        # and set additional attributes.
        self.parsed_query = urlparse(self.query)

        # Determine the root, whether it's absolute or relative
        root = "" if self.parsed_query.netloc in ["~", "."] else "/"
        self.path = IrodsPath(self.provider.session, root, self.parsed_query.netloc,
                              self.parsed_query.path.lstrip("/"))

        # Handle files with .metadata.json extensions differently.
        # The base_path is the path to the data_object/collection.
        if self.path.name.endswith(METADATA_EXT) and self.path.name != METADATA_EXT:
            self.metadata = True
            self.base_path = self.path.parent / self.path.name[:-len(METADATA_EXT)]
        else:
            self.metadata = False
            self.base_path = self.path

    async def inventory(self, cache: IOCacheStorageInterface):
        """From this file, try to find as much existence and modification date
        information as possible. Only retrieve that information that comes for free
        given the current object.
        """
        # This is optional and can be left as is

        # If this is implemented in a storage object, results have to be stored in
        # the given IOCache object, using self.cache_key() as key.
        # Optionally, this can take a custom local suffix, needed e.g. when you want
        # to cache more items than the current query: self.cache_key(local_suffix=...)
        pass

    def get_inventory_parent(self) -> Optional[str]:
        """Return the parent directory of this object."""
        # this is optional and can be left as is
        return None

    def local_suffix(self) -> str:
        """Return a unique suffix for the local path, determined from self.query."""
        return str(self.path.absolute()).lstrip("/")

    def cleanup(self):
        """Perform local cleanup of any remainders of the storage object."""
        # self.local_path() should not be removed, as this is taken care of by
        # Snakemake.
        pass

    # Fallible methods should implement some retry logic.
    # The easiest way to do this (but not the only one) is to use the retry_decorator
    # provided by snakemake-interface-storage-plugins.
    @retry_decorator
    def exists(self) -> bool:
        try:
            # Check whether the metadata exists for the base path
            if self.metadata:
                if not self.base_path.exists():
                    return False
                return len(self.base_path.meta) > 0

            # Otherwise check whether the collection/data object exists
            return self.path.exists()
        except DoesNotExistError:
            return False

    @retry_decorator
    def mtime(self) -> float:
        # TODO is this conversion needed? Unix timestamp is always UTC, right?
        ipath = self.path
        if self.metadata:
            ipath = self.base_path
        if ipath.dataobject_exists():
            return ipath.dataobject.modify_time.timestamp()
        return ipath.collection.modify_time.timestamp()

    @retry_decorator
    def size(self) -> int:
        # Return the number of metadata items
        if self.metadata:
            return len(self.base_path.meta)

        # return the size in bytes
        return self.path.size

    @retry_decorator
    def retrieve_object(self):
        if self.metadata:
            meta_dict = self.base_path.meta.to_dict()
            with open(self.local_path(), "w") as handle:
                json.dump(meta_dict["metadata"], handle)
        else:
            download(self.path, self.local_path())

    # The following to methods are only required if the class inherits from
    # StorageObjectReadWrite.

    @retry_decorator
    def store_object(self):
        if not self.path.parent.exists():
            self.path.parent.create_collection()

        if self.metadata:
            with open(self.local_path(), "r") as handle:
                meta_dict = json.load(handle)
            self.base_path.meta.from_dict({"metadata": meta_dict})
        else:
            upload(self.local_path(), self.path, overwrite=True)

    @retry_decorator
    def remove(self):
        # Remove the object from the storage.
        if self.metadata:
            self.base_path.meta.clear()
        else:
            self.path.remove()

    # The following to methods are only required if the class inherits from
    # StorageObjectGlob.

    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # This is used by glob_wildcards() to find matches for wildcards in the query.
        # The method has to return concretized queries without any remaining wildcards.
        # Use snakemake_executor_plugins.io.get_constant_prefix(self.query) to get the
        # prefix of the query before the first wildcard.
        return _find_matches(self.path, self.path.parts)

def _next_wildcard(input_str: str) -> tuple[str, Optional[str]]:
    next_index = input_str.find("{")
    if next_index == -1:
        return input_str, None
    if next_index < len(input_str)-1 and input_str[next_index+1] == "{":
        new_input_str, remaining_str = _next_wildcard(input_str[next_index+2:])
        return input_str[:next_index+2] + new_input_str, remaining_str
    last_index = input_str.find("}", next_index+1)
    if last_index == -1:
        raise ValueError(f"Bracket not closed properly in: {input_str}")
    return input_str[:next_index], input_str[last_index+1:]


def _find_matches(ipath: IrodsPath, remaining_parts: list[str]) -> list[str]:
    if len(remaining_parts) == 0:
        if ipath.exists():
            return ["irods:/" + str(ipath)]
        return []

    part = remaining_parts[0]
    new_ipath = ipath / part
    if re.match(r".*{.+}.*", part) is None:
        return _find_matches(new_ipath, remaining_parts[1:])


    if ipath.dataobject_exists() or not ipath.collection_exists():
        return []

    coll = ipath.collection
    possible_matches = {
        sub.name: sub for sub in coll.data_objects
    }
    possible_matches.update({
        sub.name: sub for sub in coll.subcollections
    })
    regex = r"^"
    cur_part = part
    while True:
        before_wc, after_wc = _next_wildcard(cur_part)
        regex += re.escape(before_wc)
        if after_wc is None:
            break
        regex += r"[\S\s]+"
        cur_part = after_wc
    regex += "$"
    matched_names = [name for name in possible_matches
                     if re.match(regex, name) is not None]

    found_matches = []
    for name in matched_names:
        found_matches.extend(_find_matches(ipath/name, remaining_parts[1:]))
    return found_matches
