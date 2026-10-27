import csv
import datetime
import io
import re
import zipfile
from collections import namedtuple
from typing import AsyncIterator, Iterable
from urllib.parse import urlencode

from scrapy import Spider
from scrapy.http import Request, Response

from locations.items import Feature
from locations.user_agents import BOT_USER_AGENT_SCRAPY

# To emit proper OpenStreetMap tags for platforms, we need to keep some
# per-station properties in memory.
Station = namedtuple("Station", "name operator operator_wikidata city country means")

# Means of transport that run on rails, as named in the feed.
RAIL_MEANS = ("CABLE_RAILWAY", "METRO", "RACK_RAILWAY", "TRAIN", "TRAM")


class OpentransportdataSwissSpider(Spider):
    name = "opentransportdata_swiss"
    allowed_domains = [
        "opentransportdata.swiss",
        "query.wikidata.org",
        "r2.cloudflarestorage.com",
    ]
    dataset_attributes = {
        # https://opentransportdata.swiss/en/authorised-databases/
        # https://opentransportdata.swiss/en/terms-of-use/#511_Exemption_for_databases
        "attribution": "optional",
        "attribution:name": "OpenTransportData.swiss",
        "attribution:wikidata": "Q97319754",
        "license:website": "https://opentransportdata.swiss/en/terms-of-use/",
        "use:commercial": "yes",
        "use:openstreetmap": "yes",
        "website": "https://opentransportdata.swiss/",
    }
    dataset_pattern = "https://data.opentransportdata.swiss/en/dataset/%s/permalink"
    custom_settings = {"ROBOTSTXT_OBEY": False}

    async def start(self) -> AsyncIterator[Request]:
        yield Request(
            "https://query.wikidata.org/sparql?{}".format(
                urlencode({"query": "SELECT ?item ?sboid WHERE {?item p:P13221 ?s. ?s ps:P13221 ?sboid.}"})
            ),
            headers={"Accept": "text/csv", "User-Agent": BOT_USER_AGENT_SCRAPY},
            callback=self.handle_wikidata_operators,
        )

    def handle_wikidata_operators(self, response: Response) -> Iterable[Request]:
        self.operators = {}
        for row in csv.DictReader(io.StringIO(response.text), delimiter=","):
            if m := re.search(r"(Q\d+)", row["item"]):
                self.operators[row["sboid"].lower().strip()] = m.group(1)
        url = "https://data.opentransportdata.swiss/de/dataset/bfr-rollstuhl"
        yield Request(url, callback=self.handle_wheelchair_overview)

    def handle_wheelchair_overview(self, response: Response) -> Iterable[Request]:
        # The download URL changes daily with every database dump.
        link = next(href for href in response.xpath("//a/@href").getall() if href.endswith("bfr_haltestellendaten.csv"))
        yield Request(link, callback=self.handle_wheelchair_data)

    def handle_wheelchair_data(self, response: Response) -> Iterable[Request]:
        # The data feed supplies seperate properties for railbound
        # and non-railbound wheelchair accessibility; in remote areas,
        # there are stations whose railway platforms are fully accessible
        # but some bus platforms haven’t yet been reconstructed to conform
        # with present-day accessibility standards. However, at this time
        # in our fetching process, we haven’t yet retrieved the full station
        # table, so we don’t yet know what means of transportations are
        # relevant. Therefore, we keep separate values for rail vs non-rail
        # wheelchair accessibility, and merge the data only later, when we
        # have fetched all data.
        #
        # The data feed sometimes supplies multiple values for wheelchair
        # accessibility, each with an optional start and end date.
        # For example, a particular platform may undergo construction
        # and still be operational during that time, although only with
        # limited accessibility for wheelchairs.
        self.wheelchair_accessible = {}
        for row in self.read_csv("bfr_haltestellendaten", response):
            sloid = row["sloid"]
            start_date = self.parse_date(row.get("gültigVon"))
            end_date = self.parse_date(row.get("gültigBis"))
            # sg = "schienengebunden" = railbound
            if row["sg_Autonomie"] == "1":
                railbound_value = "yes"
            elif row["sg_Rampeneinsatz"] == "1":
                railbound_value = "limited"
            else:
                railbound_value = "no"
            # ug = "ungebunden" = non-railbound
            if row["ug_Autonomie"] == "1":
                nonrailbound_value = "yes"
            elif row["ug_Rampeneinsatz"] == "1":
                nonrailbound_value = "limited"
            else:
                nonrailbound_value = "no"
            self.wheelchair_accessible.setdefault(sloid, []).append(
                (start_date, end_date, railbound_value, nonrailbound_value)
            )
        # Next, fetch data file for wheelchair-accessible toilets.
        url = self.dataset_pattern % "toilet-v2"
        yield Request(url, callback=self.handle_wheelchair_toilets)

    def handle_wheelchair_toilets(self, response: Response) -> Iterable[Request]:
        self.toilets_wheelchair = {}  # "ch:1:sloid:2213" -> "yes"
        for row in self.read_csv("actual-date-toilet", response):
            status = row["status"]
            sloid = row["parentSloidServicePoint"]
            value = {
                "NO": "no",
                "PARTIAL": "limited",
                "YES": "yes",
            }.get(row["wheelchairToilet"])
            if status == "VALIDATED" and sloid and value:
                self.toilets_wheelchair[sloid] = value
        self.logger.info(
            f"found {len(self.toilets_wheelchair)} " + "transit stations with wheelchair toilet attributes"
        )
        # Next, fetch data file for "service points" (stations/stop areas).
        url = self.dataset_pattern % "service-point-v2"
        yield Request(url, callback=self.handle_service_points)

    def handle_service_points(self, response: Response) -> Iterable[Feature | Request]:
        self.stations = {}
        # "swiss-only" is an misnomer; the actual data goes far beyond.
        for row in self.read_csv("actual-date-swiss-service-point", response):
            if row["status"] != "VALIDATED":
                continue
            sloid = row["sloid"]
            name = row["designationLong"] or row["designationOfficial"]
            operator = row["businessOrganisationDescriptionEn"]
            if any(operator.startswith(p) for p in ("Dummy_", "Fiktive ")):
                operator = ""
            operator_wikidata = self.operators.get(row["businessOrganisation"])
            city = row["localityName"] or row["municipalityName"]
            if not city and "," in name:
                city = name.split(",", 1)[0]
            country = row["isoCountryCode"]
            means = tuple(row["meansOfTransport"].split("|"))
            wheelchair, wheelchair_cond = self.wheelchair_tags(sloid, means)
            tags = {
                "public_transport": "stop_area",
                "name": name,
                "operator": operator,
                "operator:wikidata": operator_wikidata,
                "ref:IFOPT": sloid,
                "uic_ref": self.parse_uic_ref(row),
                "wheelchair": wheelchair,
                "wheelchair:conditional": wheelchair_cond,
                "toilets:wheelchair": self.toilets_wheelchair.get(sloid),
            }
            tags.update(self.means_tags(means, platform=None))

            lat, lon, ele = self.parse_lat_lon_ele(row)
            if lat == 0.0 or lon == 0.0:
                continue
            if ele > 0:
                tags["ele:regional"] = "%.1f" % ele

            tags = {k: v for k, v in tags.items() if v}
            yield Feature(
                lat=lat,
                lon=lon,
                city=city,
                country=country,
                ref=sloid,
                extras=tags,
            )
            self.stations[sloid] = Station(
                name=name,
                operator=operator,
                operator_wikidata=operator_wikidata,
                city=city,
                country=country,
                means=means,
            )

        # Next, fetch data for "traffic points" (platforms).
        url = self.dataset_pattern % "traffic-point-v2"
        yield Request(url, callback=self.handle_traffic_points)

    def handle_traffic_points(self, response: Response) -> Iterable[Feature]:
        # The table has two kinds of rows. A BOARDING_AREA is a whole
        # platform, such as "Perron 2" at Zürich HB. A BOARDING_PLATFORM
        # is one place where vehicles stop, such as track 3; its
        # parentSloid is its BOARDING_AREA if it has one.
        # This table does not seem to have a status column,
        # so we do not check for status="VALIDATED" here.
        rows = list(self.read_csv("actual-date-world-traffic-point", response))
        tracks = {}  # BOARDING_AREA sloid -> designations of its BOARDING_PLATFORMs
        for row in rows:
            parent = row["parentSloid"]
            if row["trafficPointElementType"] == "BOARDING_PLATFORM" and parent and row["designation"]:
                tracks.setdefault(parent, []).append(row["designation"])

        for row in rows:
            sloid = row["sloid"]
            station_sloid = row["parentSloidServicePoint"]
            if not station_sloid and sloid.startswith("ch:1:sloid:"):
                station_sloid = ":".join(sloid.split(":")[:4])
            station = self.stations.get(station_sloid)
            if not station:
                continue

            is_area = row["trafficPointElementType"] == "BOARDING_AREA"
            parent = row["parentSloid"]
            has_area = not is_area and parent and parent != station_sloid
            kind = self.platform_kind(station.means, is_area, has_area)
            if not kind:
                continue
            if is_area:
                local_ref = ";".join(sorted(set(tracks.get(sloid, [])), key=self.natural_key))
            else:
                local_ref = row["designation"]

            # Surprisingly, we don’t see in the data feed what means of
            # transportation are serving the platform, so we look at the
            # station. For wheelchair accessibility, we take the data
            # of the platform but if there’s no platform-specific data
            # we fall back to the station.
            means = station.means
            wheelchair, wheelchair_cond = self.wheelchair_tags(sloid, means)
            if not wheelchair:
                wheelchair, wheelchair_cond = self.wheelchair_tags(station_sloid, means)
            tags = {
                "name": station.name,
                "operator": station.operator,
                "operator:wikidata": station.operator_wikidata,
                "ref:IFOPT": sloid,
                "local_ref": local_ref,
                "wheelchair": wheelchair,
                "wheelchair:conditional": wheelchair_cond,
                "toilets:wheelchair": self.toilets_wheelchair.get(sloid),
            }
            tags.update(self.means_tags(station.means, platform=kind))
            lat, lon, ele = self.parse_lat_lon_ele(row)
            if lat == 0.0 or lon == 0.0:
                continue
            if ele > 0:
                tags["ele:regional"] = "%.1f" % ele
            tags = {k: v for k, v in tags.items() if v}
            yield Feature(
                lat=lat,
                lon=lon,
                city=station.city,
                country=station.country,
                ref=sloid,
                extras=tags,
            )

    @staticmethod
    def platform_kind(means: tuple[str, ...], is_area: bool, has_area: bool) -> str | None:
        """How OpenStreetMap maps a traffic point: "platform", "stop_position",
        or None for groupings that OpenStreetMap does not map."""
        rail = any(m in RAIL_MEANS for m in means)
        if is_area:
            # A rail BOARDING_AREA is a platform ("Perron"), which OpenStreetMap
            # maps as one public_transport=platform for its one or two tracks.
            # Bus areas such as "Bus" or "Alle" only group the bus stops.
            return "platform" if rail else None
        if has_area and rail:
            # A track along a Perron: OpenStreetMap puts its local_ref on the
            # railway=stop node where the train halts.
            return "stop_position"
        return "platform"

    @staticmethod
    def natural_key(s: str) -> tuple:
        return tuple(int(p) if p.isdigit() else p for p in re.split(r"(\d+)", s))

    @staticmethod
    def parse_date(d: str | None) -> datetime.date | None:
        if d and not d.startswith("9999"):
            return datetime.datetime.strptime(d, "%d.%m.%Y").date()
        else:
            return None

    @staticmethod
    def parse_lat_lon_ele(row: dict) -> tuple[float, float, float]:
        lat = round(float(row["wgs84North"] or "0.0"), 7)
        lon = round(float(row["wgs84East"] or "0.0"), 7)
        ele = float(row["height"] or "0.0")
        return lat, lon, ele

    @staticmethod
    def parse_uic_ref(row: dict) -> str | None:
        num = row["number"]
        if len(num) == 7 and num.startswith(row["uicCountryCode"]):
            return num
        else:
            return None

    @staticmethod
    def means_tags(means: tuple[str, ...], platform: str | None) -> dict[str, str]:
        """Tags for a station or stop area (platform=None), a platform
        (platform="platform"), or a stop position (platform="stop_position"),
        following how OpenStreetMap maps Swiss public transport."""
        if not means:
            return {"public_transport": platform or "stop_area"}

        # In the feed, CABLE_CAR is an aerial cable car ("Luftseilbahn")
        # and CABLE_RAILWAY a funicular ("Standseilbahn").
        tags = {
            "aerialway": "yes" if any(m in means for m in ("CABLE_CAR", "CHAIRLIFT")) else None,
            "bus": "yes" if "BUS" in means else None,
            "elevator": "yes" if "ELEVATOR" in means else None,
            "ferry": "yes" if "BOAT" in means else None,
            "funicular": "yes" if "CABLE_RAILWAY" in means else None,
            "train": "yes" if any(m in means for m in ("RACK_RAILWAY", "TRAIN")) else None,
            "tram": "yes" if "TRAM" in means else None,
            "subway": "yes" if "METRO" in means else None,
        }
        main = means[0]
        rail_main = main in RAIL_MEANS

        if platform == "stop_position":
            tags["public_transport"] = "stop_position"
            tags["railway"] = "tram_stop" if main == "TRAM" else "stop"
        elif platform == "platform":
            tags["public_transport"] = "platform"
            if "BUS" in means:
                tags["highway"] = "bus_stop"
            if rail_main or (any(m in RAIL_MEANS for m in means) and "BUS" not in means):
                tags["railway"] = "platform"
        else:
            primary_tags = {
                "BOAT": {"amenity": "ferry_terminal"},
                "CABLE_CAR": {"aerialway": "station"},
                "CHAIRLIFT": {"aerialway": "station"},
                "ELEVATOR": {"highway": "elevator"},
            }
            tags.update(primary_tags.get(main, {}))
            # Values for https://wiki.openstreetmap.org/wiki/Key:station
            station_tags = {
                "CABLE_RAILWAY": "funicular",
                "METRO": "subway",
                "RACK_RAILWAY": "railway",
                "TRAIN": "railway",
                "TRAM": "tram",
            }
            if main in station_tags:
                tags["public_transport"] = "station"
                tags["railway"] = "station"
                tags["station"] = station_tags[main]
            else:
                # OpenStreetMap maps stop areas as relations; highway=bus_stop
                # belongs on the platforms, not here.
                tags["public_transport"] = "stop_area"

        return {k: v for k, v in tags.items() if v}

    def wheelchair_tags(self, sloid: str, means: tuple[str, ...]) -> tuple[str | None, str | None]:
        """Computes the wheelchair and wheelchair:conditional tags
        for a transit station given its SLOID identifier and the available
        means of transport."""
        acc = self.wheelchair_accessible.get(sloid)
        if not acc or not means:
            return None, None
        rail_means = {"CABLE_CAR", "CABLE_RAILWAY", "METRO", "TRAIN", "TRAM"}
        has_rail = any(m in rail_means for m in means)
        has_nonrail = any(m not in rail_means for m in means)
        values = []
        for start_date, end_date, rail_value, nonrail_value in acc:
            if not start_date:
                start_date = datetime.date(1800, 1, 1)
            if not end_date:
                end_date = datetime.date(9999, 12, 31)
            if has_rail and has_nonrail:
                if rail_value == "yes" and nonrail_value == "yes":
                    combined_value = "yes"
                elif rail_value == "limited" or nonrail_value == "limited":
                    combined_value = "limited"
                else:
                    combined_value = "no"
            elif has_rail:
                combined_value = rail_value
            else:
                combined_value = nonrail_value
            values.append((start_date, end_date, combined_value))
        values.sort()
        if len(values) == 1:
            return values[0][2], None
        default_value = values[-1][2]
        cond_values = []
        for start_date, end_date, value in values:
            if value != default_value:
                start = self.format_date(start_date)
                end = self.format_date(end_date)
                cond_values.append(f"{value} @ {start} - {end}")
        return default_value, " ; ".join(cond_values)

    @staticmethod
    def format_date(d: datetime.date) -> str:
        # Python strftime() is localizing its months, whereas dates in
        # OpenStreetMap conditional expressions have month names in English.
        # Therefore, we do our own date formatting.
        months = ("Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec")
        return "%04d %s %02d" % (d.year, months[d.month - 1], d.day)

    def read_csv(self, dataset: str, response: Response) -> Iterable[dict]:
        content_type = response.headers.get("Content-Type").decode("utf-8")
        if content_type.startswith("text/csv"):
            content = response.text.removeprefix("\ufeff")
            for row in csv.DictReader(io.StringIO(content), delimiter=";"):
                yield row
        elif content_type == "application/zip":
            with zipfile.ZipFile(io.BytesIO(response.body)) as feed_zip:
                filenames = [n for n in feed_zip.namelist() if n.startswith(dataset + "-") and n.endswith(".csv")]
                if len(filenames) == 0:
                    namelist = ",".join(feed_zip.namelist())
                    self.logger.error(f'missing "{dataset}-*.csv"; ' + f"url={response.url} namelist={namelist}")
                    return
                with feed_zip.open(filenames[0]) as fp:
                    content = fp.read().decode("utf-8").removeprefix("\ufeff")
                    sio = io.StringIO(content)
                    for row in csv.DictReader(sio, delimiter=";"):
                        yield row
        else:
            self.logger.error(f"unexpected Content-Type: {content_type}")
