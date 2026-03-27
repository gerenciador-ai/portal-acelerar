const axios = require('axios');
const fs = require('fs');
const path = require('path');

const API_KEY = '29E5423CAF1E5B85B4098B699811A9D801F612E5B2781EB8033CC0BAB439EC95E4FFAA8E605F741387DE21374E42EA2DDFF191ED954DC87D10757FA4D8D8AD4F';
const PIPELINES = {
    'VMC Tech': { vendas: 110064393, churn: 110065017, onboarding: 110065015 },
    'Victec': { vendas: 110023047, churn: 110042202, onboarding: 110023069 }
};

const FIELDS = {
    VENDEDOR_REAL: 110777788,
    SDR_REAL: 110777789,
    MRR: 110778108,
    PRODUTO: 111431860,
    UPSELL: 111433407,
    ADESAO_S: 110778105,
    ADESAO_R: 111431861,
    DATA_ATIVACAO: 110778114,
    DATA_CANCELAMENTO: 111417137,
    DATA_PRIMEIRA_REUNIAO_AGENDADA: 110778042 // ID do campo 'Data da Primeira Reunião Agendada'
};

const MESES_PT = {1:'Jan',2:'Fev',3:'Mar',4:'Abr',5:'Mai',6:'Jun',7:'Jul',8:'Ago',9:'Set',10:'Out',11:'Nov',12:'Dez'};

async function fetchPloomesPaginated(endpoint, filter = '', pageSize = 200) {
    let allResults = [];
    let skip = 0;
    let hasMore = true;

    while (hasMore) {
        console.log(`📡 Buscando bloco (skip: ${skip}) para ${endpoint}...`);
        const url = `https://api2.ploomes.com/${endpoint}?$skip=${skip}&$top=${pageSize}${filter ? `&${filter}` : ''}`;
        try {
            const response = await axios.get(url, {
                headers: { 'User-Key': API_KEY, 'Content-Type': 'application/json' },
                timeout: 60000
            });
            const data = response.data.value;
            if (data && data.length > 0) {
                allResults = allResults.concat(data);
                skip += pageSize;
                console.log(`📥 Recebidos ${data.length} itens neste bloco. Total: ${allResults.length}`);
            } else {
                hasMore = false;
            }
        } catch (e) {
            console.error(`❌ Erro na paginação para ${endpoint} (skip: ${skip}):`, e.message);
            hasMore = false; // Parar em caso de erro para evitar loop infinito
        }
    }
    return allResults;
}

function sanitizeVal(v) {
    if (v === null || v === undefined || v === '') return 0;
    return parseFloat(v) || 0;
}

async function syncPloomes() {
    console.log('🚀 Iniciando Sincronização Ploomes de Alta Precisão (Modo Paginação)...');
    const allDeals = [];

    for (const empresa in PIPELINES) {
        console.log(`📦 Processando Empresa: ${empresa}...`);
        const config = PIPELINES[empresa];

        const pipelineIds = [config.vendas, config.churn, config.onboarding].filter(Boolean);
        const filter = `PipelineId in (${pipelineIds.join(',')})&$expand=OtherProperties,Owner,Contact,Status,Stage`;

        try {
            const deals = await fetchPloomesPaginated('Deals', filter);
            console.log(`✅ Total de ${deals.length} negócios brutos para ${empresa}.`);

            const mapDeal = (d) => {
                const props = d.OtherProperties || [];
                const getProp = (id) => props.find(p => p.FieldId === id);

                const vendedor = getProp(FIELDS.VENDEDOR_REAL)?.UserValueName || d.Owner?.Name || 'N/A';
                const sdr = getProp(FIELDS.SDR_REAL)?.UserValueName || 'SDR Não Identificado';

                let produto = getProp(FIELDS.PRODUTO)?.ObjectValueName || getProp(FIELDS.PRODUTO)?.ValueName || getProp(FIELDS.PRODUTO)?.Value || "Sittax Simples";
                let mrr = sanitizeVal(getProp(FIELDS.MRR)?.DecimalValue);
                let upsell = sanitizeVal(getProp(FIELDS.UPSELL)?.DecimalValue);
                let adesao = sanitizeVal(getProp(FIELDS.ADESAO_S)?.DecimalValue) || sanitizeVal(getProp(FIELDS.ADESAO_R)?.DecimalValue);

                // Lógica de data: Prioriza FinishDate, depois Data da Primeira Reunião Agendada, depois CreateDate
                const propDataAgenda = getProp(FIELDS.DATA_PRIMEIRA_REUNIAO_AGENDADA);
                let dataTrabalho;
                if (d.FinishDate) {
                    dataTrabalho = new Date(d.FinishDate);
                } else if (propDataAgenda?.DateTimeValue) {
                    dataTrabalho = new Date(propDataAgenda.DateTimeValue);
                } else {
                    dataTrabalho = new Date(d.CreateDate);
                }

                return {
                    id: d.Id,
                    cliente: d.Title,
                    cnpj: d.Contact?.CNPJ || d.Contact?.CPF || 'N/A',
                    contactId: d.ContactId,
                    data: dataTrabalho.toISOString(), // Armazenar em ISO para fácil conversão no frontend
                    dataCriacao: new Date(d.CreateDate).toISOString(),
                    vendedor: vendedor,
                    sdr: sdr,
                    mrr: mrr,
                    upsell: upsell,
                    adesao: adesao,
                    produto: produto,
                    status: d.Status?.Name || 'N/A',
                    statusId: d.StatusId,
                    estagio: d.Stage?.Name || '',
                    ano: dataTrabalho.getFullYear(),
                    mes: MESES_PT[dataTrabalho.getMonth() + 1],
                    empresa: empresa
                };
            };

            const mappedDeals = deals.map(mapDeal);
            allDeals.push(...mappedDeals);

        } catch (e) {
            console.error(`❌ Erro ao processar ${empresa}:`, e.message);
        }
    }

    // Filtrar apenas Ganhos e Perdidos para o volumeData (conforme solicitado)
    const volumeDataFiltered = allDeals.filter(d => d.statusId === 2 || d.statusId === 3);

    const finalData = {
        deals: allDeals,
        volumeData: volumeDataFiltered // Dados já filtrados para o heatmap de volume
    };

    const outputDir = path.join(__dirname, '..', 'data');
    if (!fs.existsSync(outputDir)) {
        fs.mkdirSync(outputDir, { recursive: true });
    }
    const outputPath = path.join(outputDir, 'ploomes_data.json');
    fs.writeFileSync(outputPath, JSON.stringify(finalData, null, 2), 'utf-8');
    console.log(`✅ Sucesso: Dados do Ploomes salvos em ${outputPath}`);
    console.log('✨ Sincronização Ploomes Concluída!');
}

syncPloomes();
