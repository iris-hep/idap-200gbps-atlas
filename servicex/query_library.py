import logging
from func_adl_servicex_xaodr22 import (
    FuncADLQueryPHYSLITE,
    cpp_float,
    cpp_int,
    cpp_vfloat,
    cpp_vint,
    atlas_release,
)
from servicex import FuncADLQuery


def query_all() -> FuncADLQuery:
    # Because we are going to do a specialized query, we'll alter the return type here.
    logging.info(f"Using release {atlas_release} for type information.")

    ds = FuncADLQueryPHYSLITE()

    # Build the query
    # TODO: The EventInfo argument should default correctly
    #       (that may just be a matter of using func_adl xaod r22)
    # TODO: dataclass should be supported so as not to lose type-following!
    # TODO: once https://github.com/iris-hep/func_adl/issues/136 fixed, turn off fmt off.
    # fmt: off
    query = (ds.Select(lambda e: {
            "evt": e.EventInfo("EventInfo"),
            "jet": e.Jets(),
        })
        .Select(lambda ei: {
            "event_number": ei.evt.eventNumber(),  # type: ignore
            "run_number": ei.evt.runNumber(),  # type: ignore
            "jet_pt": ei.jet.Select(lambda j: j.pt() / 1000),  # type: ignore
            "jet_eta": ei.jet.Select(lambda j: j.eta()),  # type: ignore
            "jet_phi": ei.jet.Select(lambda j: j.phi()),  # type: ignore
            "jet_m": ei.jet.Select(lambda j: j.m()),  # type: ignore
            "jet_EnergyPerSampling":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_vfloat]("EnergyPerSampling")
                ),
            "jet_SumPtTrkPt500":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_vfloat]("SumPtTrkPt500")
                ),
            "jet_TrackWidthPt1000":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_vfloat]("TrackWidthPt1000")
                ),
            "jet_NumTrkPt500":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_vint]("NumTrkPt500")
                ),
            "jet_NumTrkPt1000":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_vint]("NumTrkPt1000")
                ),
            "jet_SumPtChargedPFOPt500":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_vfloat]("SumPtChargedPFOPt500")
                ),
            "jet_Timing":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("Timing")
                ),
            "jet_JetConstitScaleMomentum_eta":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("JetConstitScaleMomentum_eta")
                ),
            "jet_ActiveArea4vec_eta":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("ActiveArea4vec_eta")
                ),
            "jet_DetectorEta":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("DetectorEta")
                ),
            "jet_JetConstitScaleMomentum_phi":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("JetConstitScaleMomentum_phi")
                ),
            "jet_ActiveArea4vec_phi":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("ActiveArea4vec_phi")
                ),
            "jet_JetConstitScaleMomentum_m":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("JetConstitScaleMomentum_m")
                ),
            "jet_JetConstitScaleMomentum_pt":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("JetConstitScaleMomentum_pt")
                ),
            "jet_Width":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("Width")
                ),
            "jet_EMFrac":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("EMFrac")
                ),
            "jet_ActiveArea4vec_m":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("ActiveArea4vec_m")
                ),
            "jet_DFCommonJets_QGTagger_TracksWidth":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("DFCommonJets_QGTagger_TracksWidth")
                ),
            "jet_JVFCorr":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("JVFCorr")
                ),
            "jet_DFCommonJets_QGTagger_TracksC1":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("DFCommonJets_QGTagger_TracksC1")
                ),
            "jet_PSFrac":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("PSFrac")
                ),
            "jet_DFCommonJets_QGTagger_NTracks":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_int]("DFCommonJets_QGTagger_NTracks")
                ),
            "jet_DFCommonJets_fJvt":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("DFCommonJets_fJvt")
                ),
            "jet_PartonTruthLabelID":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_int]("PartonTruthLabelID")
                ),
            "jet_HadronConeExclExtendedTruthLabelID":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_int]("HadronConeExclExtendedTruthLabelID")
                ),
            "jet_ConeTruthLabelID":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_int]("ConeTruthLabelID")
                ),
            "jet_HadronConeExclTruthLabelID":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_int]("HadronConeExclTruthLabelID")
                ),
            "jet_ActiveArea4vec_pt":
                ei.jet.Select(  # type: ignore
                    lambda j: j.getAttribute[cpp_float]("ActiveArea4vec_pt")
                ),
        })
    )
    # fmt: on

    return query
