ENDPOINTS = {
        "api.artifact.biosample.add": {
            "endpoint": "/api/v2/artifact/biosample/add/",
            "version": 2,
            "type": "POST",
            "scope": "majora2.add_biosampleartifact majora2.change_biosampleartifact majora2.add_biosamplesource majora2.change_biosamplesource majora2.add_biosourcesamplingprocess majora2.change_biosourcesamplingprocess",
        },

        "api.artifact.biosample.update": {
            "endpoint": "/api/v2/artifact/biosample/update/",
            "version": 2,
            "type": "POST",
            "scope": "majora2.change_biosampleartifact majora2.change_biosamplesource majora2.change_biosourcesamplingprocess",
        },

        "api.artifact.biosample.addempty": {
            "endpoint": "/api/v2/artifact/biosample/addempty/",
            "version": 2,
            "type": "POST",
            "scope": "majora2.force_add_biosampleartifact majora2.add_biosampleartifact majora2.change_biosampleartifact majora2.add_biosourcesamplingprocess majora2.change_biosourcesamplingprocess",
        },

        "api.artifact.library.add": {
            "endpoint": "/api/v2/artifact/library/add/",
            "version": 2,
            "type": "POST",
            "scope": "majora2.add_biosampleartifact majora2.change_biosampleartifact majora2.add_libraryartifact majora2.change_libraryartifact majora2.add_librarypoolingprocess majora2.change_librarypoolingprocess",
        },

        "api.process.sequencing.add": {
            "endpoint": "/api/v2/process/sequencing/add/",
            "version": 2,
            "type": "POST",
            "scope": "majora2.change_libraryartifact majora2.add_dnasequencingprocess majora2.change_dnasequencingprocess",
        },

        "api.artifact.file.add": {
            "endpoint": "/api/v2/artifact/file/add/",
            "version": 2,
            "type": "POST",
            "scope": "majora2.add_digitalresourceartifact majora2.change_digitalresourceartifact",
        },

        "api.meta.tag.add": "/api/v2/meta/tag/add/",

        "api.meta.metric.add": {
            "endpoint": "/api/v2/meta/metric/add/",
            "version": 2,
            "type": "POST",
            "scope": "majora2.add_temporarymajoraartifactmetric majora2.change_temporarymajoraartifactmetric",
        },

        "api.meta.qc.add": {
            "endpoint": "/api/v2/meta/qc/add/",
            "version": 2,
            "type": "POST",
            "scope": "majora2.add_pagqualityreport majora2.change_pagqualityreport",
        },

        "api.pag.qc.get": "/api/v2/pag/qc/get/",
        "api.pag.qc.get2": "/api/v2/pag/qc/get2/",

        "api.pag.accession.add": {
            "endpoint": "/api/v2/pag/accession/add/",
            "version": 2,
            "type": "POST",
            "scope": "majora2.add_temporaryaccessionrecord majora2.change_temporaryaccessionrecord",
        },

        "api.artifact.biosample.get": "/api/v2/artifact/biosample/get/",
        "api.process.sequencing.get": "/api/v2/process/sequencing/get/",
        "api.process.sequencing.get2": "/api/v2/process/sequencing/get2/",

        "api.artifact.biosample.query.validity": "/api/v2/artifact/biosample/query/validity/",

        "api.majora.summary.get": "/api/v2/majora/summary/get/",
        "api.outbound.summary.get": "/api/v2/outbound/summary/get/",
        "api.majora.task.get": "/api/v2/majora/task/get/",

        "api.majora.task.delete": "/api/v2/majora/task/delete/",

        "api.group.mag.get": "/api/v2/group/mag/get/",
        "api.group.pag.suppress": "/api/v2/group/pag/suppress/",

        "api.v3.majora.mdv.get": {
            "endpoint": "/api/v3/mdv/",
            "version": 3,
            "type": "GET",
            "scope": "majora2.can_read_dataview_via_api",
        },
}
