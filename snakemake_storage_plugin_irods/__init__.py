from dataclasses import dataclass, field
import datetime
import json
import os
from pathlib import PosixPath
from typing import Any, Iterable, Optional, List
from urllib.parse import urlparse
import re

# from irods.session import iRODSSession
# from irods.models import DataObject
# from irods.exception import (
#     CollectionDoesNotExist,
#     DataObjectDoesNotExist,
#     CAT_NO_ACCESS_PERMISSION,
#     CAT_NAME_EXISTS_AS_DATAOBJ,
# )
# import irods.keywords as kw

from ibridges.interactive import interactive_auth
from ibridges import IrodsPath, get_dataobject, get_collection, download, upload

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


env_msg = "Value in ~/.irods/irods_environment.json has higher priority, if present. "
pswd_msg = (
    "If no password is provided in the settings or environment file, the"
    + " password in ~/.irods/.irodsA will be used with native authentication. "
)


@dataclass
class StorageProviderSettings(StorageProviderSettingsBase):
    pass
    # host: Optional[str] = field(
    #     default=None,
    #     metadata={
    #         "help": f"The host name of the iRODS server. {env_msg}",
    #         "env_var": False,
    #         "required": True,
    #     },
    # )
    # port: Optional[int] = field(
    #     default=None,
    #     metadata={
    #         "help": f"The port of the iRODS server. {env_msg}",
    #         "env_var": False,
    #         "required": True,
    #     },
    # )
    # username: Optional[str] = field(
    #     default=None,
    #     metadata={
    #         "help": f"The user name for the iRODS server. {env_msg}",
    #         "env_var": True,
    #         "required": True,
    #     },
    # )
    # password: Optional[str] = field(
    #     default=None,
    #     metadata={
    #         "help": f"The password for the iRODS server. {env_msg}",
    #         "env_var": True,
    #         "required": True,
    #     },
    # )
    # zone: Optional[str] = field(
    #     default=None,
    #     metadata={
    #         "help": f"The zone for the iRODS server. {env_msg}",
    #         "env_var": False,
    #         "required": True,
    #     },
    # )
    # home: Optional[str] = field(
    #     default=None,
    #     metadata={
    #         "help": f"The home parameter for the iRODS server. {env_msg}",
    #         "env_var": False,
    #         "required": True,
    #     },
    # )
    # authentication_scheme: str = field(
    #     default="native",
    #     metadata={
    #         "help": f"The authentication scheme for the iRODS server. {env_msg}",
    #         "env_var": False,
    #         "required": True,
    #     },
    # )

    # def __post_init__(self):
    #     env_file = PosixPath(os.path.expanduser("~/.irods/irods_environment.json"))
    #     if env_file.exists():
    #         with open(env_file) as f:
    #             env = json.load(f)

    #         def retrieve(src, trgt):
    #             if getattr(self, trgt) is None:
    #                 setattr(self, trgt, env[src])

    #         retrieve("irods_host", "host")
    #         retrieve("irods_port", "port")
    #         retrieve("irods_user_name", "username")
    #         retrieve("irods_password", "password")
    #         retrieve("irods_zone_name", "zone")
    #         retrieve("irods_authentication_scheme", "authentication_scheme")
    #         retrieve("irods_home", "home")


utc = datetime.datetime.fromtimestamp(0, datetime.timezone.utc)


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
        self.session = interactive_auth()
        # self.session = iRODSSession(
        #     host=self.settings.host,
        #     port=self.settings.port,
        #     user=self.settings.username,
        #     password=self.settings.password,
        #     zone=self.settings.zone,
        #     authentication_scheme=self.settings.authentication_scheme,
        # )

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
        return self.settings.host

    def default_max_requests_per_second(self) -> float:
        """Return the default maximum number of requests per second for this storage
        provider."""
        return 100.0

    def use_rate_limiter(self) -> bool:
        """Return False if no rate limiting is needed for this provider."""
        return False

    @classmethod
    def is_valid_query(cls, query: str) -> StorageQueryValidationResult:
        """Return whether the given query is valid for this storage provider."""
        # Ensure that also queries containing wildcards (e.g. {sample}) are accepted
        # and considered valid. The wildcards will be resolved before the storage
        # object is actually used.
        parsed = urlparse(query)
        if parsed.scheme == "irods" and parsed.path:
            return StorageQueryValidationResult(valid=True, query=query)
        else:
            return StorageQueryValidationResult(
                valid=False,
                query=query,
                reason="Query does not start with irods:// or does not "
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
        if self.parsed_query.netloc == "~":
            self.path = IrodsPath(self.provider.session, self.parsed_query.netloc,
                                  self.parsed_query.path.lstrip("/"))
        else:
            self.path = IrodsPath(self.provider.session, self.query[7:])
        # self.path = PosixPath(
        #     f"/{self.parsed_query.netloc}"
        # ) / self.parsed_query.path.lstrip("/")

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
        return self.path.absolute_path().lstrip("/")

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
        # TODO does this also work for collections?
        # return True if the object exists
        return self.path.collection_exists() or self.path.dataobject_exists()

    # def _data_obj(self):
        # return 
        # return self.provider.session.data_objects.get(str(self.path))

    @retry_decorator
    def mtime(self) -> float:
        # TODO does this also work for collections (i.e. directories)?
        # return the modification time
        # meta = self.provider.session.metadata.get(DataObject, str(self.path))
        # for m in meta:
        #     if m.name == "mtime":
        #         return float(m.value)
        # TODO is this conversion needed? Unix timestamp is always UTC, right?
        # dt = self._convert_time(self._data_obj().modify_time, timezone)
        if self.path.dataobject_exists():
            return get_dataobject(self.provider.session, self.path).modify_time.timestamp()
        return get_collection(self.provider.session, self.path).modify_time.timestamp()
        # return self._data_obj().modify_time.timestamp()

    @retry_decorator
    def size(self) -> int:
        # return the size in bytes
        return get_dataobject(self.provider.session, self.path).size

    @retry_decorator
    def retrieve_object(self):
        download(self.provider.session, self.path, self.local_path())
        # Ensure that the object is accessible locally under self.local_path()
        # opts = {kw.FORCE_FLAG_KW: ""}
        # try:
        #     # is directory
        #     collection = self.provider.session.collections.get(str(self.path))
        #     for _, _, objs in collection.walk():
        #         for obj in objs:
        #             self.provider.session.data_objects.get(
        #                 obj.path, str(self.local_path() / obj.path), options=opts
        #             )
        # except CollectionDoesNotExist:
        #     # is file
        #     self.provider.session.data_objects.get(
        #         str(self.path), str(self.local_path()), options=opts
        #     )

    # The following to methods are only required if the class inherits from
    # StorageObjectReadWrite.

    @retry_decorator
    def store_object(self):
        upload(self.provider.session, self.local_path(), self.path, overwrite=True)
        # Ensure that the object is stored at the location specified by
        # self.local_path().
        # def mkdir(path):
        #     try:
        #         self.provider.session.collections.get(path)
        #     except CAT_NO_ACCESS_PERMISSION:
        #         pass
        #     except CollectionDoesNotExist:
        #         self.provider.session.collections.create(path)

        # for parent in self.path.parents[:-2][::-1]:
        #     mkdir(str(parent))

        # if self.local_path().is_dir():
        #     mkdir(str(self.path))
        #     for f in self.local_path().iterdir():
        #         self.provider.session.data_objects.put(str(f), str(self.path / f.name))
        # else:
        #     self.provider.session.data_objects.put(
        #         str(self.local_path()), str(self.path)
        #     )

    @retry_decorator
    def remove(self):
        # Remove the object from the storage.
        self.path.remove()
        # try:
        #     self.provider.session.collections.unregister(str(self.path))
        # except CAT_NAME_EXISTS_AS_DATAOBJ:
        #     self.provider.session.data_objects.unregister(str(self.path))

    # The following to methods are only required if the class inherits from
    # StorageObjectGlob.

    @retry_decorator
    def list_candidate_matches(self) -> Iterable[str]:
        """Return a list of candidate matches in the storage for the query."""
        # This is used by glob_wildcards() to find matches for wildcards in the query.
        # The method has to return concretized queries without any remaining wildcards.
        # Use snakemake_executor_plugins.io.get_constant_prefix(self.query) to get the
        # prefix of the query before the first wildcard.
        return _find_matches(IrodsPath(self.provider.session, self.path),
                             self.path._path.parts)
        # cur_ipath = IrodsPath(self.provider.session, "/")
        # for part in self.path._path.parts:
            # if re.match(r".*{.+}.*", part) is None:
                # cur_ipath.joinpath(part)
                # continue
            
            
    def _convert_time(self, timestamp, tz=None):
        dt = timestamp.replace(tzinfo=datetime.timezone("UTC"))
        if tz:
            dt = dt.astimezone(datetime.timezone(tz))
        return dt


def _next_wildcard(input_str: str) -> tuple[str, Optional[str]]:
    next_index = input_str.find("{")
    if next_index == -1:
        return input_str, None
    if next_index < len(input_str)-1 and input_str[next_index+1] == "{":
        new_input_str, remaining_str = _next_wildcard(input_str[next_index+2:])
        return input_str[:next_index+2] + new_input_str, remaining_str
    last_index = input_str.find("}")
    if last_index == -1:
        raise ValueError(f"Bracket not closed properly in: {input_str}")
    return input_str[:next_index], input_str[last_index+1:]
        

def _find_matches(ipath: IrodsPath, remaining_parts: list[str]):# -> list[str]:
    if len(remaining_parts) == 0:
        if ipath.exists():
            return [str(ipath)]
        return []


    part = remaining_parts[0]
    new_ipath = ipath / part
    if re.match(r".*{.+}.*", part) is None:
        return _find_matches(new_ipath, remaining_parts[1:])

    if ipath.dataobject_exists() or not ipath.collection_exists():
        return []

    coll = get_collection(ipath.session, ipath)
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
        regex += before_wc
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
    # return [
            # for name ]
        
    # print(regex)
        # for name, item in possible_matches.items():
            
        # possible_matches = {name: item for name, item in possible_matches.items()
                            # if name.startswith(before_wc)}
        
    

    # else:
        # if 