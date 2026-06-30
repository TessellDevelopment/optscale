from katara.katara_worker.reports_generators.base import Base


class PoolExceed(Base):
    def generate(self):
        user_id = self.report_data.get('user_id')
        _, organization = self.rest_cl.organization_get(self.organization_id)
        exceeded_pools_keys = [
            'exceeded_pools', 'exceeded_pool_forecasts']
        _, tasks = self.rest_cl.my_tasks_get(
            self.organization_id, user_id=user_id,
            types=exceeded_pools_keys)
        exceeded = []
        for exceeded_pools_key in exceeded_pools_keys:
            exceeded_pools = [
                {
                    'id': x['pool_id'],
                    'limit': x['limit'],
                    'total_expenses': round(x['total_expenses'], 2),
                    'pool_name': x['pool_name'],
                    'forecast': x['forecast']
                } for x in tasks.get(exceeded_pools_key, {}).get('tasks', [])
            ]
            exceeded.extend(exceeded_pools)
        if not exceeded:
            return
        # Compute totals from the scoped pools visible to this recipient
        total_cost = round(sum(p['total_expenses'] for p in exceeded), 2)
        total_forecast = round(sum(p['forecast'] for p in exceeded), 2)

        result = {
            'email': [self.report_data['user_email']],
            'template_type': self.get_template_type(__file__),
            'subject': (
                f'Action Required: {self.config_cl.company_name()} '
                f'{self.config_cl.product_name()} Pool Limit Exceed Alert'
            ),
            'template_params': {
                'texts': {
                    'organization': {
                        'id': organization['id'],
                        'name': organization['name'],
                        'currency_code': self.get_currency_code(
                            organization['currency'])
                    },
                    'exceeded': exceeded,
                    'total_cost': total_cost,
                    'total_forecast': total_forecast
                }
            }
        }
        return result


def main(organization_id, report_data, config_client):
    return PoolExceed(organization_id, report_data, config_client).generate()
