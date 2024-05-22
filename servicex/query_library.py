import logging
from typing import Tuple
from func_adl_servicex_xaodr22 import (
    FuncADLQueryPHYSLITE,
    cpp_float,
    cpp_int,
    cpp_vfloat,
    cpp_vint,
    atlas_release,
)
from servicex import FuncADLQuery


def build_query(name: str) -> Tuple[FuncADLQuery, str]:
    if name == "xaod_all":
        return (query_xaod_all(), "atlasr22")
    elif name == "xaod_medium":
        return (query_xaod_medium(), "atlasr22")
    elif name == "xaod_small":
        return (query_xaod_small(), "atlasr22")
    else:
        raise ValueError(f"Unknown query type {name}")


def query_xaod_all() -> FuncADLQuery:
    return build_xaod_query("all")


def query_xaod_medium() -> FuncADLQuery:
    return build_xaod_query("medium")


def query_xaod_small() -> FuncADLQuery:
    return build_xaod_query("small")


def build_xaod_query(q_type: str) -> FuncADLQuery:
    # Because we are going to do a specialized query, we'll alter the return type here.
    logging.info(f"Using release {atlas_release} for type information.")

    ds = FuncADLQueryPHYSLITE()

    # Build the query
    event_model = ds.Select(
        lambda e: {
            "evt": e.EventInfo("EventInfo"),
            "jet": e.Jets(),
            "ele": e.Electrons(),
            "muons": e.Muons(),
            "pv": e.Vertices("PrimaryVertices"),
        }
    )

    # Apply slimming and skimming
    def skim_jets(events, jet_cut: float):
        return events.Select(
            lambda e: {
                "evt": e.evt,
                "jet": e.jet.Where(
                    lambda j: (j.pt() / 1000 > jet_cut) and (abs(j.eta()) < 2.5)
                ),
                "ele": e.ele,
                "muons": e.muons,
                "pv": e.pv,
            }
        )

    def count_jets(events, n_jets: int):
        return events.Where(lambda e: len(e.jet) >= n_jets)

    if q_type == "medium":
        event_model = skim_jets(event_model, 25.0)

    if q_type == "small":
        event_model = skim_jets(event_model, 50.0)

    if q_type == "small" or q_type == "medium":
        event_model = count_jets(event_model, 4)

    # Get out the data we need
    query = event_model.Select(
        lambda ei: {
            "event_number": ei.evt.eventNumber(),  # type: ignore
            "run_number": ei.evt.runNumber(),  # type: ignore
            "jet_pt": ei.jet.Select(lambda j: j.pt() / 1000),  # type: ignore
            "jet_eta": ei.jet.Select(lambda j: j.eta()),  # type: ignore
            "jet_phi": ei.jet.Select(lambda j: j.phi()),  # type: ignore
            "jet_m": ei.jet.Select(lambda j: j.m()),  # type: ignore
            "jet_EnergyPerSampling": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_vfloat]("EnergyPerSampling")
            ),
            "jet_SumPtTrkPt500": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_vfloat]("SumPtTrkPt500")
            ),
            "jet_TrackWidthPt1000": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_vfloat]("TrackWidthPt1000")
            ),
            "jet_NumTrkPt500": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_vint]("NumTrkPt500")
            ),
            "jet_NumTrkPt1000": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_vint]("NumTrkPt1000")
            ),
            "jet_SumPtChargedPFOPt500": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_vfloat]("SumPtChargedPFOPt500")
            ),
            "jet_Timing": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("Timing")
            ),
            "jet_JetConstitScaleMomentum_eta": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("JetConstitScaleMomentum_eta")
            ),
            "jet_JetConstitScaleMomentum_m": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("JetConstitScaleMomentum_m")
            ),
            "jet_JetConstitScaleMomentum_pt": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("JetConstitScaleMomentum_pt")
            ),
            "jet_JetConstitScaleMomentum_phi": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("JetConstitScaleMomentum_phi")
            ),
            "jet_ActiveArea4vec_eta": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("ActiveArea4vec_eta")
            ),
            "jet_ActiveArea4vec_phi": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("ActiveArea4vec_phi")
            ),
            "jet_ActiveArea4vec_m": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("ActiveArea4vec_m")
            ),
            "jet_ActiveArea4vec_pt": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("ActiveArea4vec_pt")
            ),
            "jet_DetectorEta": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("DetectorEta")
            ),
            "jet_Width": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("Width")
            ),
            "jet_EMFrac": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("EMFrac")
            ),
            "jet_DFCommonJets_QGTagger_TracksWidth": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("DFCommonJets_QGTagger_TracksWidth")
            ),
            "jet_DFCommonJets_QGTagger_TracksC1": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("DFCommonJets_QGTagger_TracksC1")
            ),
            "jet_DFCommonJets_QGTagger_NTracks": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_int]("DFCommonJets_QGTagger_NTracks")
            ),
            "jet_DFCommonJets_fJvt": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("DFCommonJets_fJvt")
            ),
            "jet_PSFrac": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("PSFrac")
            ),
            "jet_JVFCorr": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_float]("JVFCorr")
            ),
            # "jet_NNJvtPass": ei.jet.Select(  # type: ignore
            #     lambda j: j.getAttribute[cpp_char]("NNJvtPass")
            # ),
            "jet_GhostMuonSegmentCount": ei.jet.Select(  # type: ignore
                lambda j: j.getAttribute[cpp_int]("GhostMuonSegmentCount")
            ),
            "ele_pt": ei.ele.Select(lambda e: e.pt() / 1000),  # type: ignore
            "ele_eta": ei.ele.Select(lambda e: e.eta()),  # type: ignore
            "ele_phi": ei.ele.Select(lambda e: e.phi()),  # type: ignore
            "ele_m": ei.ele.Select(lambda e: e.m() / 1000),  # type: ignore
            "muons_pt": ei.muons.Select(lambda m: m.pt() / 1000),  # type: ignore
            "muons_eta": ei.muons.Select(lambda m: m.pt()),  # type: ignore
            "muons_phi": ei.muons.Select(lambda m: m.pt()),  # type: ignore
            "muons_m": ei.muons.Select(lambda m: m.m() / 1000),  # type: ignore
            # "AnalysisMuonsAuxDyn.muonSegmentLinks",
            # "AnalysisMuonsAuxDyn.msOnlyExtrapolatedMuonSpectrometerTrackParticleLink",
            # "AnalysisMuonsAuxDyn.extrapolatedMuonSpectrometerTrackParticleLink",
            # "AnalysisMuonsAuxDyn.inDetTrackParticleLink",
            # "AnalysisMuonsAuxDyn.muonSpectrometerTrackParticleL
            "pv_x": ei.pv.Select(lambda p: p.x() / 1000),  # type: ignore
            "pv_y": ei.pv.Select(lambda p: p.y() / 1000),  # type: ignore
            "pv_z": ei.pv.Select(lambda p: p.z() / 1000),  # type: ignore
            # "PrimaryVerticesAuxDyn.vertexType",
        }
    )

    return query
