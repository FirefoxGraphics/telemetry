import json
import collections
from .snake_case import SnakeCaseDict


def revert(s):
    replacements = {
        "client_id": "clientId",
        "creation_date": "creationDate",
        "keyed_histograms": "keyedHistograms",
        "build_id": "buildId",
        "user_prefs": "userPrefs",
        "service_pack_major": "servicePackMajor",
        "is_wow64": "isWow64",
        "memory_mb": "memoryMB",
    }
    if s in replacements:
        return replacements[s]
    else:
        return s


def convert_bigquery_results(f):
    "Convert dataframe-row rdd record into a format suitable for get_pings_properties, which does histogram conversion"
    d = f.asDict(True)
    # generally speaking, we could use additional_properties as the basis and correctly implement recursive merging
    # instead, we explicitly add the properties we know we care about at the end of this routine
    additional_properties = json.loads(d.pop("additional_properties") or "{}")
    newdict = {}
    for k, v in d.items():
        pieces = list(map(revert, k.split("__")))
        # print(pieces)
        if len(pieces) == 1:
            newdict[pieces[0]] = v
        elif len(pieces) == 3:
            first, second, third = pieces
            if second == "histograms" and isinstance(v, str):
                v = json.loads(v)
            if second == "keyedHistograms":
                if len(v) == 0:
                    continue
                else:
                    z = {}
                    for i in v:
                        z[i["key"]] = json.loads(i["value"])
                    v = z
            if first not in newdict:
                newdict[first] = {}
            if second not in newdict[first]:
                newdict[first][second] = {}
            newdict[first][second][third] = v
        elif len(pieces) == 4:
            first, second, third, fourth = pieces
            if first not in newdict:
                newdict[first] = {}
            if second not in newdict[first]:
                newdict[first][second] = {}
            if third not in newdict[first][second]:
                newdict[first][second][third] = {}
            newdict[first][second][third][fourth] = v
        elif len(pieces) == 5:
            first, second, third, fourth, fifth = pieces
            if fourth == "histograms" and isinstance(v, str):
                v = json.loads(v)
            if fourth == "keyedHistograms":
                if len(v) == 0:
                    continue
                else:
                    z = {}
                    for i in v:
                        z[i["key"]] = json.loads(i["value"])
                    v = z
            if first not in newdict:
                newdict[first] = {}
            if second not in newdict[first]:
                newdict[first][second] = {}
            if third not in newdict[first][second]:
                newdict[first][second][third] = {}
            if fourth not in newdict[first][second][third]:
                newdict[first][second][third][fourth] = {}
            newdict[first][second][third][fourth][fifth] = v
        elif len(pieces) > 5:
            raise (Exception("too many pieces"))

    # example additional_properties
    # {"environment":{"system":{"gfx":{"adapters":[{"driverVendor":null}],"ContentBackend":"Skia"}},"addons":{"activeGMPlugins":{"dummy-gmp":{"applyBackgroundUpdates":1}}}},"payload":{"processes":{"extension":{"histograms":{"FXA_CONFIGURED":{"bucket_count":3,"histogram_type":3,"sum":0,"range":[1,2],"values":{"0":1,"1":0}}}}},"simpleMeasurements":{"selectProfile":15637,"XPI_startup_end":27497,"XPI_finalUIStartup":29308,"start":696,"AMI_startup_begin":19076,"startupCrashDetectionBegin":18382,"AMI_startup_end":27766,"afterProfileLocked":15742,"startupInterrupted":0,"maximalNumberOfConcurrentThreads":72,"createTopLevelWindow":29539,"XPI_bootstrap_addons_end":27497,"XPI_bootstrap_addons_begin":27382,"sessionRestoreInitialized":29400,"delayedStartupStarted":38798,"sessionRestoreInit":29358,"debuggerAttached":0,"startupCrashDetectionEnd":75127,"delayedStartupFinished":40246,"XPI_startup_begin":19670}}}'
    if (
        "environment" in additional_properties
        and "system" in additional_properties["environment"]
        and "gfx" in additional_properties["environment"]["system"]
    ):
        gfx = additional_properties["environment"]["system"]["gfx"]
        if "ContentBackend" in gfx:
            newdict["environment"]["system"]["gfx"]["ContentBackend"] = gfx[
                "ContentBackend"
            ]
        if "adapters" in gfx:
            for i, adapter in enumerate(gfx["adapters"]):
                newdict["environment"]["system"]["gfx"]["adapters"][i].update(adapter)

    return newdict


def convert_snake_case_dict(mapping):
    """Convert mappings to SnakeCaseDicts recursively."""
    if isinstance(mapping, collections.Mapping):
        for key, value in mapping.items():
            mapping[key] = convert2(value)
        return SnakeCaseDict(mapping)
    elif isinstance(mapping, list):
        l = []
        for value in mapping:
            l.append(convert2(value))
        return l
    return mapping
