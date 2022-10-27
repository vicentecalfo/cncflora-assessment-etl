import occurrences from '../input/Atualizacao_LVFlora.json'
import fs from 'fs-extra'
import { exec } from 'child_process'
const OUTPUT_DIR = './occurrence/output/'

const fieldsMap: any = {
    family: 'family',
    id: 'systemId',
    specie_fb: 'binomialFB',
    nome_avali: 'binomial',
    col_code: 'collectionCode',
    catalog_n: 'catalogueNumber',
    recordedby: 'recordBy',
    record_n: 'recordNumber',
    year: 'year',
    month: 'month',
    day: 'day',
    state: 'state',
    city: 'city',
    locality: 'locality',
    longitude: 'longitude',
    latitude: 'latitude',
    precision: 'precision',
    protocol: 'protocol',
    obs__de_SI: 'sigComments',
    categoria: 'category',
    ano_avali: 'assessmentYear'
}

init()

async function init() {
    const sppOccurrences = createIndexedSppOccurrences({ occurrences, fieldsMap })
    await createIndexFile(sppOccurrences)
    await createIndexMapToFbFile(sppOccurrences)
    await createOccurrenceFiles(sppOccurrences)
    console.log(`Arquivo de ocorrências criado com sucesso.`)
    createEooAooFiles({ indexedSppOccurrences: sppOccurrences, index: 0 })
}

function createIndexedSppOccurrences({ occurrences, fieldsMap }: any) {
    const indexedSppOccurrences: any = {}
    occurrences.forEach((occurrence: any) => {
        const newOccurrenceRecord: any = {}
        Object.keys(fieldsMap).forEach((field: string) => {
            newOccurrenceRecord[fieldsMap[field]] = occurrence[field] === '' ? null : occurrence[field]
        })
        const index = newOccurrenceRecord.binomial
        const hasRecord = indexedSppOccurrences.hasOwnProperty(index)
        if (!hasRecord) indexedSppOccurrences[index] = []
        indexedSppOccurrences[index].push(newOccurrenceRecord)
    })
    return indexedSppOccurrences
}

function createIndexFile(indexedSppOccurrences: any) {
    return new Promise((resolve, reject) => {
        const sppsAvaiable = Object.keys(indexedSppOccurrences)
        fs.outputJsonSync(`${OUTPUT_DIR}/index-occurrences.json`, sppsAvaiable)
        console.log(`Arquivo de índice criado com sucesso.`)
        //console.log(sppsAvaiable.length)
        resolve(1)
    })
}

function createIndexMapToFbFile(indexedSppOccurrences: any) {
    return new Promise((resolve, reject) => {
        const indexedSppMapped: any = {} //nome_avaliado:nome_FB
        Object.keys(indexedSppOccurrences).forEach(spp => {
            const sppFB = indexedSppOccurrences[spp][0].binomialFB
            const hasRecord = indexedSppMapped.hasOwnProperty(spp)
            if (!hasRecord) indexedSppMapped[spp] = sppFB
        })
        fs.outputJsonSync(`${OUTPUT_DIR}/indexed-spp-mapped-to-FB.json`, indexedSppMapped)
        //console.log(Object.keys(indexedSppMapped).length)
        console.log(`Arquivo de espécies mapeadas para Flora do Brasil criado com sucesso.`)
        resolve(1)
    })
}

function createOccurrenceFiles(indexedSppOccurrences: any) {
    return new Promise((resolve, reject) => {
        const sppsAvaiable = Object.keys(indexedSppOccurrences)
        sppsAvaiable.forEach(spp => {
            const pathJson = `${OUTPUT_DIR}${spp}/occurrences.json`
            const pathCSV = `${OUTPUT_DIR}${spp}/occurrences.csv`
            fs.outputJsonSync(pathJson, indexedSppOccurrences[spp])
            fs.outputFileSync(pathCSV, utils.json2csv(indexedSppOccurrences[spp]))
            //console.log(sppsAvaiable.length)
            resolve(1)
        })
    })
}

async function createEooAooFiles({ indexedSppOccurrences, index }: any) {
    const sppsAvaiable = Object.keys(indexedSppOccurrences)
    const total = sppsAvaiable.length
    const spp = sppsAvaiable[index]
    const rowIndex = index + 1
    if (rowIndex <= total) {
        await spwanCommandEooAoo({ spp, type: 'eoo' })
        await spwanCommandEooAoo({ spp, type: 'aoo' })
        index++
        console.log(`EOO e AOO gerados para: ${spp}.`)
        console.log(rowIndex)
        createEooAooFiles({ indexedSppOccurrences, index })
    }
}

function spwanCommandEooAoo({ spp, type }: any) {
    return new Promise((resolve, reject) => {
        const commandLine = `calc-eoo-aoo ${type} -i "${OUTPUT_DIR}${spp}/occurrences.csv" -o "${OUTPUT_DIR}${spp}/" -f true`
        const ls = exec(commandLine, (error, stdout, stderr) => { })
        ls.on('exit', (code) => resolve(1))
    })
}


/* Funções Úteis */
const utils = {
    json2csv: (json: any[]) => {
        const replacer = (key: string, value: any) => value === null ? '' : value
        const header = Object.keys(json[0])
        const csv = [
            header.join(','), // header row first
            ...json.map(row => header.map(fieldName => JSON.stringify(row[fieldName], replacer)).join(','))
        ].join('\r\n')
        return csv
    }
}